-- ============================================================
-- Medallion reset — clears Bronze, Silver and Gold so the pipeline
-- can be re-run cleanly for all 4 data subjects.
--
-- Safe to re-run. Landing Parquet is NOT touched, so 01_ingest_to_bronze
-- can rebuild Bronze without re-extracting from HANA / SQL Server / Salesforce.
--
-- TRUNCATE keeps schema, partitioning and Delta history — recoverable via
-- time travel (RESTORE TABLE ... VERSION AS OF n) until VACUUM runs.
--
-- Bronze must be empty before re-running 01, which appends
-- (mode("append") in 01_ingest_to_bronze.py) and would otherwise
-- duplicate every row.
--
-- Table list verified against SHOW TABLES. Gold fact and dim tables are
-- absent — 03_silver_to_gold has not yet succeeded, and it creates them
-- on first run, so there is nothing to truncate there.
-- ============================================================

-- ── Bronze ────────────────────────────────────────────────
-- Shared by all 4 subjects, separated by the _frequency partition.
TRUNCATE TABLE hpe_catalog.bronze.o9_forecast_raw;

-- DQ quarantine — also written with append, so it accumulates across runs.
TRUNCATE TABLE hpe_catalog.bronze.quarantine;

-- ── Silver ────────────────────────────────────────────────
TRUNCATE TABLE hpe_catalog.silver.o9_forecast_ref;
TRUNCATE TABLE hpe_catalog.silver.o9_forecast_period_agg;

-- ── Gold ──────────────────────────────────────────────────
-- Orphan from an earlier version of 04_aggregated_audit, which used a single
-- shared table. Current code writes per-subject tables named
-- <subject minus o9_ prefix>_agg_audit (e.g. gold.forecast_monthly_agg_audit),
-- so nothing writes here any more. Dropped rather than truncated.
DROP TABLE IF EXISTS hpe_catalog.gold.o9_forecast_agg_audit;

-- Not truncated because they do not exist yet — 03_silver_to_gold and
-- 04_aggregated_audit create them on first successful run:
--   gold.o9_forecast_dmnsn          (fact)
--   gold.dim_product / dim_location / dim_time
--   gold.forecast_{daily,weekly,monthly,quarterly}_agg_audit

-- ── Audit logs ────────────────────────────────────────────
-- Left INTACT by default: job_log and data_quality_log are the run history
-- and are worth keeping across reruns. Uncomment only for a truly clean slate.
-- TRUNCATE TABLE hpe_catalog.audit.job_log;
-- TRUNCATE TABLE hpe_catalog.audit.data_quality_log;

-- ── Verify (all should return 0) ──────────────────────────
SELECT    'bronze.o9_forecast_raw'          AS tbl, count(*) AS rows FROM hpe_catalog.bronze.o9_forecast_raw
UNION ALL SELECT 'bronze.quarantine',             count(*) FROM hpe_catalog.bronze.quarantine
UNION ALL SELECT 'silver.o9_forecast_ref',        count(*) FROM hpe_catalog.silver.o9_forecast_ref
UNION ALL SELECT 'silver.o9_forecast_period_agg', count(*) FROM hpe_catalog.silver.o9_forecast_period_agg
ORDER BY tbl;
