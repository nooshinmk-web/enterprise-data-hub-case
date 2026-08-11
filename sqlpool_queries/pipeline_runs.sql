-- =============================================================================
-- silver.pipeline_runs — audit log of every Bronze→Silver pipeline execution
-- This is exactly the kind of table your "Data Product Trust & Freshness"
-- Power BI page (and tests/data_quality.md) should read from.
-- =============================================================================

CREATE TABLE IF NOT EXISTS dbw_datahub_case.silver.pipeline_runs
USING DELTA
LOCATION 'abfss://datahub@stdatahubcase001.dfs.core.windows.net/silver/_audit/pipeline_runs'
COMMENT 'One row per Bronze→Silver pipeline execution: counts, rejection rate, duration, status.';

-- Latest run per dataset -----------------------------------------------------
SELECT dataset_name, status, input_count, valid_count, rejected_count,
       rejection_percentage, duration_seconds, started_at
FROM dbw_datahub_case.silver.pipeline_runs
QUALIFY ROW_NUMBER() OVER (PARTITION BY dataset_name ORDER BY started_at DESC) = 1;

-- Failed runs, if any ---------------------------------------------------------
SELECT * FROM dbw_datahub_case.silver.pipeline_runs
WHERE status = 'FAILED'
ORDER BY started_at DESC;

-- Rejection trend over time (feeds a freshness/trust dashboard) --------------
SELECT dataset_name, started_at, rejection_percentage
FROM dbw_datahub_case.silver.pipeline_runs
ORDER BY started_at DESC
LIMIT 20;
