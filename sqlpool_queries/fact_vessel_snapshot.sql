-- =============================================================================
-- fact_vessel_snapshot — point-in-time vessel operating state
-- Grain: one row per vessel per snapshot_timestamp. Partitioned by snapshot_date_key.
-- =============================================================================

CREATE TABLE IF NOT EXISTS dbw_datahub_case.gold.fact_vessel_snapshot
USING DELTA
LOCATION 'abfss://datahub@stdatahubcase001.dfs.core.windows.net/gold/fact_vessel_snapshot'
COMMENT 'Vessel operating snapshots (status, draught, speed) — one row per vessel per snapshot_timestamp.';

-- Sanity checks -----------------------------------------------------------
-- Grain check
SELECT vessel_snapshot_key, COUNT(*) AS row_count
FROM dbw_datahub_case.gold.fact_vessel_snapshot
GROUP BY vessel_snapshot_key
HAVING COUNT(*) > 1;

-- Referential integrity vs dim_vessel
SELECT f.vessel_key
FROM dbw_datahub_case.gold.fact_vessel_snapshot f
LEFT ANTI JOIN dbw_datahub_case.gold.dim_vessel d
  ON f.vessel_key = d.vessel_key
LIMIT 10;

SELECT * FROM dbw_datahub_case.gold.fact_vessel_snapshot ORDER BY snapshot_timestamp DESC LIMIT 10;
