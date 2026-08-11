-- =============================================================================
-- fact_weather_forecast — weather forecast readings per location and time
-- Grain: one row per location_key per forecast_timestamp. Partitioned by forecast_date_key.
-- =============================================================================

CREATE TABLE IF NOT EXISTS dbw_datahub_case.gold.fact_weather_forecast
USING DELTA
LOCATION 'abfss://datahub@stdatahubcase001.dfs.core.windows.net/gold/fact_weather_forecast'
COMMENT 'Weather forecast readings (temperature, wind, precipitation) per location and forecast_timestamp.';

-- Sanity checks -----------------------------------------------------------
SELECT weather_forecast_key, COUNT(*) AS row_count
FROM dbw_datahub_case.gold.fact_weather_forecast
GROUP BY weather_forecast_key
HAVING COUNT(*) > 1;

-- Referential integrity vs dim_location
SELECT f.location_key
FROM dbw_datahub_case.gold.fact_weather_forecast f
LEFT ANTI JOIN dbw_datahub_case.gold.dim_location d
  ON f.location_key = d.location_key
LIMIT 10;

SELECT * FROM dbw_datahub_case.gold.fact_weather_forecast ORDER BY forecast_timestamp DESC LIMIT 10;
