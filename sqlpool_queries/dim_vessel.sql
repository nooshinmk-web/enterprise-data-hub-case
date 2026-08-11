-- =============================================================================
-- dim_vessel — SCD Type 2 vessel master dimension
-- Registers the Delta files written by 03_silver_to_gold as a Unity Catalog table
-- so it's queryable via SQL and discoverable from Power BI / Tableau.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS dbw_datahub_case.gold
COMMENT 'Gold layer — dimensional model for maritime & weather data product';

CREATE TABLE IF NOT EXISTS dbw_datahub_case.gold.dim_vessel
USING DELTA
LOCATION 'abfss://datahub@stdatahubcase001.dfs.core.windows.net/gold/dim_vessel'
COMMENT 'Vessel master dimension with full change history (SCD Type 2). One row per vessel per attribute-version.';

-- Sanity checks -----------------------------------------------------------
-- Only one current row per vessel_key
SELECT vessel_key, COUNT(*) AS current_versions
FROM dbw_datahub_case.gold.dim_vessel
WHERE is_current = true
GROUP BY vessel_key
HAVING COUNT(*) > 1;

-- Preview
SELECT * FROM dbw_datahub_case.gold.dim_vessel WHERE is_current = true LIMIT 10;
