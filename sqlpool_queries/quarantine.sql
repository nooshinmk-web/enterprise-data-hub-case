-- =============================================================================
-- silver._quarantine — records that failed data-quality rules
-- Registered as tables ONLY if the quarantine path already has data
-- (the notebook only writes here when rejected_count > 0, so these tables
--  may not exist yet on a clean run — that's expected, not an error).
-- =============================================================================

CREATE TABLE IF NOT EXISTS dbw_datahub_case.silver.quarantine_vessel
USING DELTA
LOCATION 'abfss://datahub@stdatahubcase001.dfs.core.windows.net/silver/_quarantine/vessel'
COMMENT 'Vessel records rejected by data-quality rules, with failed_quality_rules and quarantined_at.';

CREATE TABLE IF NOT EXISTS dbw_datahub_case.silver.quarantine_weather
USING DELTA
LOCATION 'abfss://datahub@stdatahubcase001.dfs.core.windows.net/silver/_quarantine/weather'
COMMENT 'Weather records rejected by data-quality rules, with failed_quality_rules and quarantined_at.';

-- Inspect what's failing and why -------------------------------------------
SELECT failed_quality_rules, COUNT(*) AS row_count
FROM dbw_datahub_case.silver.quarantine_vessel
GROUP BY failed_quality_rules
ORDER BY row_count DESC;

SELECT failed_quality_rules, COUNT(*) AS row_count
FROM dbw_datahub_case.silver.quarantine_weather
GROUP BY failed_quality_rules
ORDER BY row_count DESC;
