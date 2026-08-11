"""
Silver layer pipeline for the Enterprise Data Hub technical case.

Reads raw vessel and weather JSON from Bronze, applies transformations
and data-quality rules, writes valid records to Silver Delta tables,
quarantines rejected rows, and stores a simple audit record per run.

The code is intentionally reusable for both datasets where possible.
All ingestion and processing timestamps are kept in UTC.
"""

import uuid

from datetime import datetime, timezone
from delta.tables import DeltaTable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    explode_outer,
    filter as array_filter,
    lit,
    lower,
    regexp_extract,
    sha2,
    size,
    struct,
    to_date,
    to_json,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# =============================================================================
# SECTION 1 — CONFIGURATION
# =============================================================================
# Storage configuration
# Environment-specific values are passed by the Databricks Job so the same
# notebook can be reused across environments.
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container", "")

STORAGE_ACCOUNT = dbutils.widgets.get("storage_account").strip()
CONTAINER = dbutils.widgets.get("container").strip()

if not STORAGE_ACCOUNT or not CONTAINER:
    raise ValueError(
        "Required job parameters 'storage_account' and 'container' must be provided."
    )

BRONZE_BASE_PATH = (
    f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}"
    ".dfs.core.windows.net/bronze"
)

SILVER_BASE_PATH = (
    f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}"
    ".dfs.core.windows.net/silver"
)


# Silver paths
VESSEL_TARGET_PATH = (
    f"{SILVER_BASE_PATH}/vessel"
)

WEATHER_TARGET_PATH = (
    f"{SILVER_BASE_PATH}/weather"
)

VESSEL_QUARANTINE_PATH = (
    f"{SILVER_BASE_PATH}/_quarantine/vessel"
)

WEATHER_QUARANTINE_PATH = (
    f"{SILVER_BASE_PATH}/_quarantine/weather"
)

AUDIT_PATH = (
    f"{SILVER_BASE_PATH}/_audit/pipeline_runs"
)

def read_bronze_json(
    spark: SparkSession,
    dataset_name: str,
) -> DataFrame:
    """
    Read raw JSON files written by ADF and extract
    ingestion metadata from the filename.

    Expected filename:
    vessel_20260803_214137.json
    """

    source_path = (
        f"{BRONZE_BASE_PATH}/{dataset_name}/"
    )

    return (
        spark.read
        .option("multiLine", "true")
        .json(source_path)
        .withColumn(
            "source_name",
            lit(dataset_name),
        )
        .withColumn(
            "source_file",
            col("_metadata.file_path"),
        )
        .withColumn(
            "ingestion_timestamp",
            to_timestamp(
                regexp_extract(
                    col("_metadata.file_path"),
                    r"_(\d{8}_\d{6})\.json$",
                    1,
                ),
                "yyyyMMdd_HHmmss",
            ),
        )
        .withColumn(
            "ingestion_date",
            to_date(
                col("ingestion_timestamp")
            ),
        )
    )
df_vessel_bronze = read_bronze_json(
    spark=spark,
    dataset_name="vessel",
)

df_weather_bronze = read_bronze_json(
    spark=spark,
    dataset_name="weather",
)
# =============================================================================
# SECTION 2 — SHARED / REUSABLE FUNCTIONS
# =============================================================================
# Record hash used for idempotent change detection
def add_record_hash(
    df: DataFrame,
    business_columns: list[str],
) -> DataFrame:
    """Add a SHA-256 hash across business columns for change detection."""
    return df.withColumn(
        "record_hash",
        sha2(
            to_json(
                struct(
                    *[
                        col(column_name)
                        for column_name in business_columns
                    ]
                )
            ),
            256,
        ),
    )
# Data quality and quarantine

def apply_quality_rules(
    df: DataFrame,
    rules: list[tuple[str, object]],
) -> tuple[DataFrame, DataFrame]:
    """Split a dataframe into valid and quarantined records."""

    failed_rules = array_filter(
        array(
            *[
                when(
                    ~coalesce(condition, lit(False)),
                    lit(rule_name),
                )
                for rule_name, condition in rules
            ]
        ),
        lambda rule: rule.isNotNull(),
    )

    checked_df = df.withColumn(
        "failed_quality_rules",
        failed_rules,
    )

    valid_df = (
        checked_df
        .filter(
            size(col("failed_quality_rules")) == 0
        )
        .drop("failed_quality_rules")
    )

    quarantine_df = (
        checked_df
        .filter(
            size(col("failed_quality_rules")) > 0
        )
        .withColumn(
            "quarantined_at",
            current_timestamp(),
        )
    )

    return valid_df, quarantine_df
# Idempotent Delta merge

def merge_to_delta(
    df: DataFrame,
    target_path: str,
    merge_keys: list[str],
) -> None:
    """Create or merge into a Delta table without duplicating existing keys."""

    if not DeltaTable.isDeltaTable(
        spark,
        target_path,
    ):
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
        )
        return

    merge_condition = " AND ".join(
        [
            f"target.`{key}` <=> source.`{key}`"
            for key in merge_keys
        ]
    )

    (
        DeltaTable.forPath(spark, target_path).alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll(condition=("NOT (target.record_hash <=> source.record_hash)"))
        .whenNotMatchedInsertAll()
        .execute()
    )
AUDIT_SCHEMA = StructType([
    StructField(
        "pipeline_run_id",
        StringType(),
        False,
    ),
    StructField(
        "dataset_name",
        StringType(),
        False,
    ),
    StructField(
        "status",
        StringType(),
        False,
    ),
    StructField(
        "input_count",
        LongType(),
        False,
    ),
    StructField(
        "transformed_count",
        LongType(),
        False,
    ),
    StructField(
        "valid_count",
        LongType(),
        False,
    ),
    StructField(
        "rejected_count",
        LongType(),
        False,
    ),
    StructField(
        "rejection_percentage",
        DoubleType(),
        False,
    ),
    StructField(
        "started_at",
        TimestampType(),
        False,
    ),
    StructField(
        "finished_at",
        TimestampType(),
        False,
    ),
    StructField(
        "duration_seconds",
        DoubleType(),
        False,
    ),
    StructField(
        "error_message",
        StringType(),
        True,
    ),
])
def write_audit_record(
    pipeline_run_id: str,
    dataset_name: str,
    status: str,
    input_count: int,
    transformed_count: int,
    valid_count: int,
    rejected_count: int,
    started_at: datetime,
    finished_at: datetime,
    error_message: str | None = None,
) -> None:
    """Append one pipeline execution record to the Silver audit table."""

    rejection_percentage = (
        rejected_count * 100 / transformed_count
        if transformed_count
        else 0.0
    )

    duration_seconds = (finished_at - started_at).total_seconds()

    audit_record = {
        "pipeline_run_id": pipeline_run_id,
        "dataset_name": dataset_name,
        "status": status,
        "input_count": input_count,
        "transformed_count": transformed_count,
        "valid_count": valid_count,
        "rejected_count": rejected_count,
        "rejection_percentage": rejection_percentage,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": duration_seconds,
        "error_message": error_message,
    }

    (
        spark.createDataFrame([audit_record], schema=AUDIT_SCHEMA)
        .write
        .format("delta")
        .mode("append")
        .save(AUDIT_PATH)
    )
# =============================================================================
# SECTION 3 — VESSEL TRANSFORMATION
# =============================================================================

def transform_vessel(
    df: DataFrame,
) -> DataFrame:
    """Flatten and standardize the raw VesselAPI payload."""

    result = (
        df.select(
            col("vessel.imo")
            .cast("long")
            .alias("imo"),

            col("vessel.mmsi")
            .cast("long")
            .alias("mmsi"),

            trim(
                col("vessel.name")
            ).alias("vessel_name"),

            trim(
                col("vessel.name_ais")
            ).alias("vessel_name_ais"),

            trim(
                col("vessel.call_sign")
            ).alias("call_sign"),

            trim(
                col("vessel.country")
            ).alias("country"),

            lower(
                trim(col("vessel.country_code"))
            ).alias("country_code"),

            trim(
                col("vessel.vessel_type")
            ).alias("vessel_type"),

            trim(
                col("vessel.operating_status")
            ).alias("operating_status"),

            col("vessel.length")
            .cast("double")
            .alias("length"),

            trim(
                col("vessel.length_unit")
            ).alias("length_unit"),

            col("vessel.breadth")
            .cast("double")
            .alias("breadth"),

            trim(
                col("vessel.breadth_unit")
            ).alias("breadth_unit"),

            col("vessel.draught_calculated_avg")
            .cast("double")
            .alias("draught_avg"),

            col("vessel.draught_observed_max")
            .cast("double")
            .alias("draught_max"),

            col("vessel.speed_calculated_avg")
            .cast("double")
            .alias("speed_avg"),

            col("vessel.speed_observed_max")
            .cast("double")
            .alias("speed_max"),

            col("ingestion_timestamp"),

            to_date(
                col("ingestion_date")
            ).alias("ingestion_date"),
        )
        .withColumn(
            "vessel_id",
            when(
                col("imo").isNotNull(),
                concat_ws(
                    "-",
                    lit("IMO"),
                    col("imo"),
                ),
            ).when(
                col("mmsi").isNotNull(),
                concat_ws(
                    "-",
                    lit("MMSI"),
                    col("mmsi"),
                ),
            ),
        )
    )

    business_columns = [
        column_name
        for column_name in result.columns
        if column_name not in ("ingestion_date", "ingestion_timestamp")
    ]

    return (
        add_record_hash(
            result,
            business_columns,
        )
        .withColumn(
            "silver_processed_at",
            current_timestamp(),
        )
        .dropDuplicates([
            "vessel_id",
            "ingestion_timestamp",
        ])
    )


df_vessel_silver = transform_vessel(
    df_vessel_bronze
)
vessel_quality_rules = [
    (
        "missing_vessel_identifier",
        col("vessel_id").isNotNull(),
    ),
    (
        "invalid_ingestion_date",
        col("ingestion_date").isNotNull(),
    ),
    (
        "invalid_length",
        col("length").isNull() | col("length").between(0, 500)
    ),
    (
        "invalid_breadth",
        col("breadth").isNull() | col("breadth").between(0, 100)
    ),
    (
        "invalid_draught",
        col("draught_avg").isNull() | col("draught_avg").between(0, 40)
    ),
    (
        "invalid_speed",
        col("speed_avg").isNull() | col("speed_avg").between(0, 100),
    ),
]
# =============================================================================
# SECTION 4 — WEATHER TRANSFORMATION
# =============================================================================
# Weather transformation
def transform_weather(
    df: DataFrame,
) -> DataFrame:
    """Flatten the weather timeseries payload into forecast-level rows."""

    result = (
        df
        .withColumn(
            "forecast",
            explode_outer(
                col("properties.timeseries")
            ),
        )
        .select(
            col("geometry.coordinates")[0]
            .cast("double")
            .alias("longitude"),

            col("geometry.coordinates")[1]
            .cast("double")
            .alias("latitude"),

            to_timestamp(
                col("forecast.time")
            ).alias("forecast_timestamp"),

            to_timestamp(
                col("properties.meta.updated_at")
            ).alias("weather_updated_at"),

            col(
                "forecast.data.instant.details"
                ".air_temperature"
            )
            .cast("double")
            .alias("air_temperature"),

            col(
                "forecast.data.instant.details"
                ".air_pressure_at_sea_level"
            )
            .cast("double")
            .alias("air_pressure"),

            col(
                "forecast.data.instant.details"
                ".cloud_area_fraction"
            )
            .cast("double")
            .alias("cloud_area_fraction"),

            col(
                "forecast.data.instant.details"
                ".relative_humidity"
            )
            .cast("double")
            .alias("relative_humidity"),

            col(
                "forecast.data.instant.details"
                ".wind_from_direction"
            )
            .cast("double")
            .alias("wind_direction"),

            col(
                "forecast.data.instant.details"
                ".wind_speed"
            )
            .cast("double")
            .alias("wind_speed"),

            col(
                "forecast.data.next_1_hours"
                ".details.precipitation_amount"
            )
            .cast("double")
            .alias("precipitation_1h"),

            col(
                "forecast.data.next_1_hours"
                ".summary.symbol_code"
            ).alias("weather_symbol_1h"),

            col(
                "forecast.data.next_6_hours"
                ".details.precipitation_amount"
            )
            .cast("double")
            .alias("precipitation_6h"),

            col(
                "forecast.data.next_12_hours"
                ".summary.symbol_code"
            ).alias("weather_symbol_12h"),

            to_date(
                col("ingestion_date")
            ).alias("ingestion_date"),
        )
        .withColumn(
            "weather_id",
            sha2(
                concat_ws(
                    "||",
                    col("longitude"),
                    col("latitude"),
                    col("forecast_timestamp"),
                ),
                256,
            ),
        )
    )

    business_columns = [
        column_name
        for column_name in result.columns
        if column_name != "ingestion_date"
    ]

    return (
        add_record_hash(
            result,
            business_columns,
        )
        .withColumn(
            "silver_processed_at",
            current_timestamp(),
        )
        .dropDuplicates(["weather_id"])
    )
df_weather_silver = transform_weather(
    df_weather_bronze
)
weather_quality_rules = [
    (
        "missing_forecast_timestamp",
        col("forecast_timestamp").isNotNull(),
    ),
    (
        "invalid_ingestion_date",
        col("ingestion_date").isNotNull(),
    ),
    (
        "invalid_longitude",
        col("longitude").between(-180, 180),
    ),
    (
        "invalid_latitude",
        col("latitude").between(-90, 90),
    ),
    (
        "invalid_relative_humidity",
        col("relative_humidity").isNull()
        | col("relative_humidity").between(0, 100),
    ),
    (
        "invalid_cloud_area_fraction",
        col("cloud_area_fraction").isNull()
        | col("cloud_area_fraction").between(0, 100),
    ),
    (
        "invalid_wind_speed",
        col("wind_speed").isNull()
        | (col("wind_speed") >= 0),
    ),
    (
        "invalid_precipitation",
        col("precipitation_1h").isNull()
        | (col("precipitation_1h") >= 0),
    ),
]
# =============================================================================
# SECTION 5 — PIPELINE RUNNER
# =============================================================================
# Shared pipeline runner
def run_silver_pipeline(
    dataset_name: str,
    source_df: DataFrame,
    transformed_df: DataFrame,
    quality_rules: list[tuple[str, object]],
    target_path: str,
    quarantine_path: str,
    merge_keys: list[str],
    rejection_threshold: float = 20.0,
) -> None:
    """Apply quality checks, merge valid rows, quarantine rejects, and audit the run."""

    pipeline_run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    input_count = 0
    transformed_count = 0
    valid_count = 0
    rejected_count = 0

    valid_df = None
    quarantine_df = None

    print(
        f"Starting {dataset_name} pipeline "
        f"run_id={pipeline_run_id}"
    )

    try:
        input_count = source_df.count()

        valid_df, quarantine_df = (
            apply_quality_rules(
                transformed_df,
                quality_rules,
            )
        )

        valid_count = valid_df.count()
        rejected_count = quarantine_df.count()

        transformed_count = (
            valid_count + rejected_count
        )

        rejection_percentage = (
            rejected_count
            / transformed_count
            * 100
            if transformed_count > 0
            else 0.0
        )

        if rejection_percentage > rejection_threshold:
            raise ValueError(
                "Data-quality threshold exceeded: "
                f"{rejection_percentage:.2f}% rejected; "
                f"allowed={rejection_threshold:.2f}%"
            )

        merge_to_delta(
            df=valid_df,
            target_path=target_path,
            merge_keys=merge_keys,
        )

        if rejected_count > 0:
            (
                quarantine_df
                .withColumn(
                    "pipeline_run_id",
                    lit(pipeline_run_id),
                )
                .write
                .format("delta")
                .mode("append")
                .save(quarantine_path)
            )

        finished_at = datetime.now(timezone.utc)

        write_audit_record(
            pipeline_run_id=pipeline_run_id,
            dataset_name=dataset_name,
            status="SUCCESS",
            input_count=input_count,
            transformed_count=transformed_count,
            valid_count=valid_count,
            rejected_count=rejected_count,
            started_at=started_at,
            finished_at=finished_at,
        )

        print(
            f"Completed {dataset_name}: "
            f"input={input_count}, "
            f"valid={valid_count}, "
            f"rejected={rejected_count}"
        )

    except Exception as error:
        finished_at = datetime.now(timezone.utc)

        write_audit_record(
            pipeline_run_id=pipeline_run_id,
            dataset_name=dataset_name,
            status="FAILED",
            input_count=input_count,
            transformed_count=transformed_count,
            valid_count=valid_count,
            rejected_count=rejected_count,
            started_at=started_at,
            finished_at=finished_at,
            error_message=str(error)[:4000],
        )

        print(
            f"Pipeline failed: {dataset_name}"
        )

        raise


# =============================================================================
# SECTION 6 — EXECUTION
# =============================================================================

run_silver_pipeline(
    dataset_name="vessel",
    source_df=df_vessel_bronze,
    transformed_df=df_vessel_silver,
    quality_rules=vessel_quality_rules,
    target_path=VESSEL_TARGET_PATH,
    quarantine_path=VESSEL_QUARANTINE_PATH,
    merge_keys=[
        "vessel_id",
        "ingestion_timestamp",
    ],
)
run_silver_pipeline(
    dataset_name="weather",
    source_df=df_weather_bronze,
    transformed_df=df_weather_silver,
    quality_rules=weather_quality_rules,
    target_path=WEATHER_TARGET_PATH,
    quarantine_path=WEATHER_QUARANTINE_PATH,
    merge_keys=["weather_id"],
)
