"""Gold layer transformation for the Enterprise Data Hub technical case.

Builds Delta Lake dimension and fact tables from Silver vessel and weather data.
Designed to run in Azure Databricks, where a global SparkSession named `spark`
is available.
"""

# =============================================================================
# GOLD LAYER PIPELINE — Vessel & Weather
# Improvements over baseline:
#   1. Generic, reusable functions for dimension/fact building (DRY)
#   2. True SCD Type 2 for dim_vessel (effective_start_date, effective_end_date, is_current)
#   3. Incremental weather read from Silver; vessel snapshot read remains full in this prototype
#   4. Deterministic dedup (window + row_number instead of dropDuplicates)
#   5. Partitioning + OPTIMIZE/ZORDER on fact tables for query performance
#   6. Broadcast joins for referential integrity checks against small dims
#   7. Null / data-quality validation on business keys before hashing
#   8. Schema evolution support (mergeSchema) on writes
# =============================================================================

from datetime import datetime
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    broadcast,
    col,
    concat_ws,
    current_timestamp,
    date_format,
    dayofmonth,
    dayofweek,
    lit,
    month,
    quarter,
    row_number,
    sha2,
    struct,
    to_json,
    weekofyear,
    year,
)
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
# Environment-specific values are injected through Databricks Job parameters.
# This keeps Azure resource names out of source control and allows the same
# code to be reused across dev/test/prod environments.
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container", "")

STORAGE_ACCOUNT = dbutils.widgets.get("storage_account").strip()
CONTAINER = dbutils.widgets.get("container").strip()

if not STORAGE_ACCOUNT or not CONTAINER:
    raise ValueError(
        "Required job parameters 'storage_account' and 'container' must be provided."
    )

SILVER_BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver"
GOLD_BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/gold"

VESSEL_SILVER_PATH = f"{SILVER_BASE_PATH}/vessel"
WEATHER_SILVER_PATH = f"{SILVER_BASE_PATH}/weather"

DIM_VESSEL_PATH = f"{GOLD_BASE_PATH}/dim_vessel"          # SCD2
DIM_LOCATION_PATH = f"{GOLD_BASE_PATH}/dim_location"       # SCD1
DIM_DATE_PATH = f"{GOLD_BASE_PATH}/dim_date"                # SCD1
FACT_VESSEL_SNAPSHOT_PATH = f"{GOLD_BASE_PATH}/fact_vessel_snapshot"
FACT_WEATHER_FORECAST_PATH = f"{GOLD_BASE_PATH}/fact_weather_forecast"

# =============================================================================
# SECTION 1 — SHARED / REUSABLE FUNCTIONS
# =============================================================================

def add_record_hash(df: DataFrame, columns: list[str]) -> DataFrame:
    """Hash a set of columns to a single change-detection column."""
    return df.withColumn(
        "record_hash",
        sha2(to_json(struct(*[col(c) for c in columns])), 256),
    )


def validate_not_null(df: DataFrame, columns: list[str], table_name: str) -> None:
    """Fail fast if business/merge keys contain nulls — prevents silent data loss."""
    for column_name in columns:
        null_count = df.filter(col(column_name).isNull()).limit(1).count()
        if null_count > 0:
            raise ValueError(
                f"Null value found in required column '{column_name}' of {table_name}"
            )


def validate_unique_key(df: DataFrame, keys: list[str], table_name: str) -> None:
    has_duplicate = (
        df.groupBy(*keys).count().filter(col("count") > 1).limit(1).count() > 0
    )
    if has_duplicate:
        raise ValueError(f"Duplicate key detected in {table_name}: {keys}")


def validate_referential_integrity(
    fact_df: DataFrame, dim_df: DataFrame, fk_column: str, table_name: str
) -> None:
    """Broadcast the (small) dimension for a cheap anti-join integrity check."""
    orphan_count = (
        fact_df.select(fk_column)
        .join(broadcast(dim_df.select(fk_column)), on=fk_column, how="left_anti")
        .limit(1)
        .count()
    )
    if orphan_count > 0:
        raise ValueError(f"{table_name} contains {fk_column} values missing from dimension")


def deduplicate_latest(
    df: DataFrame, partition_cols: list[str], order_col: str
) -> DataFrame:
    """
    Deterministic dedup: keep the most recent row per partition key.
    Replaces non-deterministic dropDuplicates() when a natural ordering exists.
    """
    window_spec = Window.partitionBy(*partition_cols).orderBy(col(order_col).desc())
    return (
        df.withColumn("_row_number", row_number().over(window_spec))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
    )


def get_last_processed_timestamp(spark, target_path: str, ts_column: str = "gold_processed_at") -> Optional[datetime]:
    """
    Read the current max watermark already in gold so silver reads can be incremental.
    Returns None if the gold table doesn't exist yet (first / full run).
    """
    if not DeltaTable.isDeltaTable(spark, target_path):
        return None
    result = (
        spark.read.format("delta").load(target_path)
        .agg({ts_column: "max"})
        .collect()[0][0]
    )
    return result


def read_silver_incremental(
    spark, path: str, watermark_column: str, since: Optional[datetime] = None
) -> DataFrame:
    """Incremental read using a watermark column; falls back to full read on first run."""
    df = spark.read.format("delta").load(path)
    if since is not None:
        df = df.filter(col(watermark_column) > lit(since))
    return df


def merge_scd1_to_gold(df: DataFrame, target_path: str, merge_keys: list[str]) -> None:
    """Standard upsert (no history) — used for dim_location, dim_date, and facts."""
    if not DeltaTable.isDeltaTable(spark, target_path):
        df.write.format("delta").mode("overwrite").save(target_path)
        return

    merge_condition = " AND ".join(
        [f"target.`{key}` <=> source.`{key}`" for key in merge_keys]
    )
    (
        DeltaTable.forPath(spark, target_path)
        .alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll(
            condition="NOT (target.record_hash <=> source.record_hash)"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_fact_to_gold(
    df: DataFrame, target_path: str, merge_keys: list[str], partition_col: str
) -> None:
    """Fact merge with partitioning + schema evolution for scalability."""
    if not DeltaTable.isDeltaTable(spark, target_path):
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy(partition_col)
            .option("mergeSchema", "true")
            .save(target_path)
        )
        return

    merge_condition = " AND ".join(
        [f"target.`{key}` <=> source.`{key}`" for key in merge_keys]
    )
    (
        DeltaTable.forPath(spark, target_path)
        .alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll(
            condition="NOT (target.record_hash <=> source.record_hash)"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_scd2_to_gold(
    df_source: DataFrame, target_path: str, business_key: list[str]
) -> None:
    """
    Classic two-step SCD Type 2 merge:
      Step A — close out current rows whose record_hash changed (is_current=false, set end date)
      Step B — insert new current versions for changed + brand-new business keys
    Requires df_source to already carry a `record_hash` column (see add_record_hash).
    """
    if not DeltaTable.isDeltaTable(spark, target_path):
        (
            df_source
            .withColumn("effective_start_date", current_timestamp())
            .withColumn("effective_end_date", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
            .write.format("delta")
            .mode("overwrite")
            .partitionBy("is_current")
            .save(target_path)
        )
        return

    delta_target = DeltaTable.forPath(spark, target_path)
    target_current = delta_target.toDF().filter(col("is_current"))

    key_join_condition = business_key  # list of column names, equi-join

    changed_or_new = (
        df_source.alias("s")
        .join(
            target_current.select(*business_key, col("record_hash").alias("target_hash")).alias("t"),
            on=key_join_condition,
            how="left",
        )
        .filter(col("target_hash").isNull() | (col("record_hash") != col("target_hash")))
        .select("s.*")
    )

    if changed_or_new.limit(1).count() == 0:
        return  # nothing changed, skip merge entirely

    match_condition = " AND ".join(
        [f"target.`{k}` <=> source.`{k}`" for k in business_key]
    )

    # Step A — close out changed current rows
    (
        delta_target.alias("target")
        .merge(changed_or_new.alias("source"), f"{match_condition} AND target.is_current = true")
        .whenMatchedUpdate(
            set_={
                "is_current": "false",
                "effective_end_date": "current_timestamp()",
            }
        )
        .execute()
    )

    # Step B — insert new current versions
    new_versions = (
        changed_or_new
        .withColumn("effective_start_date", current_timestamp())
        .withColumn("effective_end_date", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
    )
    new_versions.write.format("delta").mode("append").option("mergeSchema", "true").save(target_path)


def optimize_table(spark, target_path: str, zorder_cols: list[str]) -> None:
    """Compact small files + Z-ORDER for faster point/range lookups on hot columns."""
    zorder_expr = ", ".join(zorder_cols)
    spark.sql(f"OPTIMIZE delta.`{target_path}` ZORDER BY ({zorder_expr})")
# =============================================================================
# SECTION 2 — READ SILVER (INCREMENTAL)
# =============================================================================

vessel_watermark_col = "ingestion_timestamp"  # fallback handled below if absent

_last_vessel_ts = get_last_processed_timestamp(spark, FACT_VESSEL_SNAPSHOT_PATH)
_last_weather_ts = get_last_processed_timestamp(spark, FACT_WEATHER_FORECAST_PATH)

df_vessel_silver_full = spark.read.format("delta").load(VESSEL_SILVER_PATH)
if "ingestion_timestamp" not in df_vessel_silver_full.columns:
    vessel_watermark_col = "ingestion_date"

df_vessel_silver = spark.read.format("delta").load(VESSEL_SILVER_PATH)
df_weather_silver = read_silver_incremental(
    spark, WEATHER_SILVER_PATH, "weather_updated_at", since=_last_weather_ts
)

vessel_snapshot_column = (
    col("ingestion_timestamp")
    if vessel_watermark_col == "ingestion_timestamp"
    else col("ingestion_date").cast("timestamp")
)

# -----------------------------------------------------------------------------
# Data quality: fail fast on missing business keys before we hash / merge anything
# -----------------------------------------------------------------------------
validate_not_null(df_vessel_silver, ["vessel_id"], "vessel_silver")
validate_not_null(df_weather_silver, ["longitude", "latitude", "forecast_timestamp"], "weather_silver")
# =============================================================================
# SECTION 3 — DIM_VESSEL (SCD TYPE 2)
# =============================================================================

vessel_dedup_window_df = deduplicate_latest(
    df_vessel_silver.withColumn("_snap_ts", vessel_snapshot_column),
    partition_cols=["vessel_id"],
    order_col="_snap_ts",
).drop("_snap_ts")

vessel_attribute_cols = [
    "imo", "mmsi", "vessel_name", "vessel_name_ais", "call_sign",
    "country", "country_code", "vessel_type",
    "length", "length_unit", "breadth", "breadth_unit",
]

dim_vessel_source = (
    vessel_dedup_window_df
    .select(
        sha2(col("vessel_id"), 256).alias("vessel_key"),  # durable business key (hash)
        "vessel_id",
        *vessel_attribute_cols,
    )
)

dim_vessel_source = add_record_hash(dim_vessel_source, vessel_attribute_cols)

merge_scd2_to_gold(
    df_source=dim_vessel_source,
    target_path=DIM_VESSEL_PATH,
    business_key=["vessel_key"],
)

dim_vessel_current = (
    spark.read.format("delta").load(DIM_VESSEL_PATH).filter(col("is_current"))
)
# =============================================================================
# SECTION 4 — DIM_LOCATION & DIM_DATE (SCD TYPE 1 — attributes don't need history)
# =============================================================================

weather_with_location = df_weather_silver.withColumn(
    "location_key", sha2(concat_ws("||", col("longitude"), col("latitude")), 256)
)

dim_location = (
    weather_with_location
    .select("location_key", "longitude", "latitude")
    .dropDuplicates(["location_key"])
)
dim_location = add_record_hash(dim_location, ["longitude", "latitude"]).withColumn(
    "gold_processed_at", current_timestamp()
)

fact_vessel_snapshot = (
     df_vessel_silver.withColumn("snapshot_timestamp", vessel_snapshot_column)
    .withColumn("vessel_key", sha2(col("vessel_id"), 256))
    .withColumn(
        "snapshot_date_key",
        date_format(col("snapshot_timestamp"), "yyyyMMdd").cast("int"),
    )
    .withColumn(
        "vessel_snapshot_key",
        sha2(concat_ws("||", col("vessel_id"), col("snapshot_timestamp")), 256),
    )
    .select(
        "vessel_snapshot_key", "vessel_key", "snapshot_date_key", "snapshot_timestamp",
        "operating_status", "draught_avg", "draught_max", "speed_avg", "speed_max",
    )
)
fact_vessel_snapshot = add_record_hash(
    fact_vessel_snapshot,
    ["operating_status", "draught_avg", "draught_max", "speed_avg", "speed_max"],
).withColumn("gold_processed_at", current_timestamp())
fact_vessel_snapshot = deduplicate_latest(
    fact_vessel_snapshot, ["vessel_snapshot_key"], "gold_processed_at"
)

fact_weather_forecast = (
    weather_with_location
    .withColumn(
        "forecast_date_key",
        date_format(col("forecast_timestamp"), "yyyyMMdd").cast("int"),
    )
    .withColumn(
        "weather_forecast_key",
        sha2(concat_ws("||", col("location_key"), col("forecast_timestamp")), 256),
    )
    .select(
        "weather_forecast_key", "location_key", "forecast_date_key", "forecast_timestamp",
        "weather_updated_at", "air_temperature", "air_pressure", "cloud_area_fraction",
        "relative_humidity", "wind_direction", "wind_speed", "precipitation_1h",
        "precipitation_6h", "weather_symbol_1h", "weather_symbol_12h",
    )
)
weather_attribute_cols = [
    "air_temperature", "air_pressure", "cloud_area_fraction", "relative_humidity",
    "wind_direction", "wind_speed", "precipitation_1h", "precipitation_6h",
    "weather_symbol_1h", "weather_symbol_12h",
]
fact_weather_forecast = add_record_hash(fact_weather_forecast, weather_attribute_cols).withColumn(
    "gold_processed_at", current_timestamp()
)
fact_weather_forecast = deduplicate_latest(
    fact_weather_forecast, ["weather_forecast_key"], "gold_processed_at"
)

vessel_dates = fact_vessel_snapshot.select(col("snapshot_timestamp").cast("date").alias("full_date"))
weather_dates = fact_weather_forecast.select(col("forecast_timestamp").cast("date").alias("full_date"))

dim_date = (
    vessel_dates.union(weather_dates)
    .filter(col("full_date").isNotNull())
    .dropDuplicates(["full_date"])
    .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year(col("full_date")))
    .withColumn("quarter", quarter(col("full_date")))
    .withColumn("month", month(col("full_date")))
    .withColumn("week_of_year", weekofyear(col("full_date")))
    .withColumn("day_of_month", dayofmonth(col("full_date")))
    .withColumn("day_of_week", dayofweek(col("full_date")))
    .withColumn("is_weekend", dayofweek(col("full_date")).isin(1, 7))
)
dim_date = add_record_hash(
    dim_date,
    ["full_date", "year", "quarter", "month", "week_of_year", "day_of_month", "day_of_week", "is_weekend"],
).withColumn("gold_processed_at", current_timestamp())

# =============================================================================
# SECTION 5 — VALIDATION (uniqueness + referential integrity, before any write)
# =============================================================================

validate_unique_key(dim_vessel_current, ["vessel_key"], "dim_vessel (current)")
validate_unique_key(dim_location, ["location_key"], "dim_location")
validate_unique_key(dim_date, ["date_key"], "dim_date")
validate_unique_key(fact_vessel_snapshot, ["vessel_snapshot_key"], "fact_vessel_snapshot")
validate_unique_key(fact_weather_forecast, ["weather_forecast_key"], "fact_weather_forecast")

validate_referential_integrity(
    fact_vessel_snapshot, dim_vessel_current, "vessel_key", "fact_vessel_snapshot"
)
validate_referential_integrity(
    fact_weather_forecast, dim_location, "location_key", "fact_weather_forecast"
)

# =============================================================================
# SECTION 6 — WRITE (dims are SCD1/SCD2 already handled above for dim_vessel)
# =============================================================================

merge_scd1_to_gold(dim_location, DIM_LOCATION_PATH, ["location_key"])
merge_scd1_to_gold(dim_date, DIM_DATE_PATH, ["date_key"])

merge_fact_to_gold(
    fact_vessel_snapshot, FACT_VESSEL_SNAPSHOT_PATH,
    ["vessel_snapshot_key"], partition_col="snapshot_date_key",
)
merge_fact_to_gold(
    fact_weather_forecast, FACT_WEATHER_FORECAST_PATH,
    ["weather_forecast_key"], partition_col="forecast_date_key",
)

# -----------------------------------------------------------------------------
# Periodic maintenance (run less frequently, e.g. nightly job — shown here for completeness)
# -----------------------------------------------------------------------------
optimize_table(spark, FACT_VESSEL_SNAPSHOT_PATH, zorder_cols=["vessel_key"])
optimize_table(spark, FACT_WEATHER_FORECAST_PATH, zorder_cols=["location_key"])

# -----------------------------------------------------------------------------
# Notes
# -----------------------------------------------------------------------------
# - All processing timestamps are stored in UTC.
# - Secrets are not stored in this module.
# - OPTIMIZE/ZORDER is included for demonstration; in production it should be
#   scheduled separately from every ingestion run.
