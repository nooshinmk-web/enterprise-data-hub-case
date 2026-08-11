-- =============================================================================
-- dim_location — SCD Type 1 dimension of distinct weather observation coordinates
-- =============================================================================

CREATE TABLE IF NOT EXISTS dbw_datahub_case.gold.dim_location
USING DELTA
LOCATION 'abfss://datahub@stdatahubcase001.dfs.core.windows.net/gold/dim_location'
COMMENT 'Distinct latitude/longitude points used by weather forecasts. SCD Type 1 — no history needed.';

-- Sanity checks -----------------------------------------------------------
SELECT location_key, COUNT(*) AS row_count
FROM dbw_datahub_case.gold.dim_location
GROUP BY location_key
HAVING COUNT(*) > 1;

SELECT * FROM dbw_datahub_case.gold.dim_location LIMIT 10;
