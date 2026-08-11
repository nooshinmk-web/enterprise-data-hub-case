-- =============================================================================
-- silver.weather — cleaned, exploded weather forecast data
-- Registers the Delta files written by 02_bronze_to_silver_weather.py
-- =============================================================================

CREATE TABLE IF NOT EXISTS dbw_datahub_case.silver.weather
USING DELTA
LOCATION 'abfss://datahub@stdatahubcase001.dfs.core.windows.net/silver/weather'
COMMENT 'Weather forecast records after timeseries explode, schema enforcement, quality rules, and dedup by weather_id.';

-- Sanity checks -----------------------------------------------------------
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT weather_id) AS distinct_readings
FROM dbw_datahub_case.silver.weather;

SELECT weather_id, COUNT(*) AS row_count
FROM dbw_datahub_case.silver.weather
GROUP BY weather_id
HAVING COUNT(*) > 1;

SELECT * FROM dbw_datahub_case.silver.weather ORDER BY silver_processed_at DESC LIMIT 10;
