-- =============================================================================
-- silver.vessel — cleaned, deduplicated vessel data
-- Registers the Delta files written by 01_bronze_to_silver_vessel.py
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS dbw_datahub_case.silver
COMMENT 'Silver layer — cleaned, deduplicated, conformed data';

CREATE TABLE IF NOT EXISTS dbw_datahub_case.silver.vessel
USING DELTA
LOCATION 'abfss://datahub@stdatahubcase001.dfs.core.windows.net/silver/vessel'
COMMENT 'Vessel records after schema enforcement, quality rules, and dedup by vessel_id + ingestion_date.';

-- Sanity checks -----------------------------------------------------------
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT vessel_id) AS distinct_vessels
FROM dbw_datahub_case.silver.vessel;

-- Duplicate check on the actual merge key (vessel_id + ingestion_date)
SELECT vessel_id, ingestion_date, COUNT(*) AS row_count
FROM dbw_datahub_case.silver.vessel
GROUP BY vessel_id, ingestion_date
HAVING COUNT(*) > 1;

SELECT * FROM dbw_datahub_case.silver.vessel ORDER BY silver_processed_at DESC LIMIT 10;
