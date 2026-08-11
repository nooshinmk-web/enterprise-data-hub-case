-- =============================================================================
-- dim_date — standard calendar dimension, SCD Type 1
-- =============================================================================

CREATE TABLE IF NOT EXISTS dbw_datahub_case.gold.dim_date
USING DELTA
LOCATION 'abfss://datahub@stdatahubcase001.dfs.core.windows.net/gold/dim_date'
COMMENT 'Calendar dimension spanning all dates present in vessel snapshots and weather forecasts.';

-- Sanity checks -----------------------------------------------------------
SELECT date_key, COUNT(*) AS row_count
FROM dbw_datahub_case.gold.dim_date
GROUP BY date_key
HAVING COUNT(*) > 1;

SELECT * FROM dbw_datahub_case.gold.dim_date ORDER BY full_date LIMIT 10;
