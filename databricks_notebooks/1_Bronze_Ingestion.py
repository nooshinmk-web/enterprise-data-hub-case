"""
Bronze layer helper for the Enterprise Data Hub technical case.

Reads raw JSON files written by Azure Data Factory into the Bronze area
and adds ingestion metadata derived from the file name.

This notebook does not modify the raw payload. It only exposes the raw
records in a consistent shape for downstream Silver processing.
All ingestion timestamps are kept in UTC.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    regexp_extract,
    to_date,
    to_timestamp,
)


# =============================================================================
# SECTION 1 — CONFIGURATION
# =============================================================================

# Environment-specific values are passed by the Databricks Job so the same
# notebook can be reused across dev/test/prod environments.
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


# =============================================================================
# SECTION 2 — SHARED / REUSABLE FUNCTIONS
# =============================================================================

def read_bronze_json(
    spark: SparkSession,
    dataset_name: str,
) -> DataFrame:
    """
    Read raw JSON files written to Bronze by ADF and extract
    ingestion metadata from the file name.

    Expected naming pattern:
        <dataset>_yyyyMMdd_HHmmss.json
    """
    source_path = f"{BRONZE_BASE_PATH}/{dataset_name}/"

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
            to_date(col("ingestion_timestamp")),
        )
    )


# =============================================================================
# SECTION 3 — VESSEL BRONZE VIEW
# =============================================================================

df_vessel_bronze = (
    read_bronze_json(
        spark=spark,
        dataset_name="vessel",
    )
    .select(
        "vessel",
        "source_name",
        "source_file",
        "ingestion_timestamp",
        "ingestion_date",
    )
)


# =============================================================================
# SECTION 4 — WEATHER BRONZE VIEW
# =============================================================================

df_weather_bronze = (
    read_bronze_json(
        spark=spark,
        dataset_name="weather",
    )
    .select(
        "geometry",
        "properties",
        "type",
        "source_name",
        "source_file",
        "ingestion_timestamp",
        "ingestion_date",
    )
)


# =============================================================================
# SECTION 5 — NOTE
# =============================================================================
# This Bronze notebook intentionally keeps the source payload unchanged.
# Data cleansing, validation, deduplication, quarantine handling, and Delta
# upserts are handled in the Silver layer.
