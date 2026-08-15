-- ============================================================
-- Medallion reset — clears Silver and Gold so the pipeline can be re-run
-- cleanly for all 4 data subjects.
--
-- Landing Parquet is NOT touched, so 01_landing_to_silver can rebuild without
-- re-extracting from HANA / SQL Server / Salesforce.
--
-- TRUNCATE keeps schema, partitioning and Delta history — recoverable via
-- time travel (RESTORE TABLE ... VERSION AS OF n) until VACUUM runs.
--
-- The Bronze layer was removed: landing is the raw archive, so the extra Delta
-- hop added no guarantee. See 13_drop_bronze_layer.sql to clear it out.
-- ============================================================

-- ── Silver ────────────────────────────────────────────────
-- Shared by all 4 subjects, separated by the _frequency partition.
TRUNCATE TABLE hpe_catalog.silver.o9_forecast_ref;
TRUNCATE TABLE hpe_catalog.silver.o9_forecast_period_agg;

-- Structural-DQ rejects. Also append-only, so it accumulates across runs.
TRUNCATE TABLE hpe_catalog.silver.quarantine;

-- ── Gold ──────────────────────────────────────────────────
-- Dropped rather than truncated so the dimensions rebuild from scratch: a
-- TRUNCATE would leave surrogate keys from an earlier run behind.
DROP TABLE IF EXISTS hpe_catalog.gold.fact_forecast;
DROP TABLE IF EXISTS hpe_catalog.gold.dim_product;
DROP TABLE IF EXISTS hpe_catalog.gold.dim_location;
DROP TABLE IF EXISTS hpe_catalog.gold.dim_time;

-- Per-subject aggregated audit tables written by 04_aggregated_audit.
DROP TABLE IF EXISTS hpe_catalog.gold.forecast_daily_agg_audit;
DROP TABLE IF EXISTS hpe_catalog.gold.forecast_weekly_agg_audit;
DROP TABLE IF EXISTS hpe_catalog.gold.forecast_monthly_agg_audit;
DROP TABLE IF EXISTS hpe_catalog.gold.forecast_quarterly_agg_audit;

-- ── Audit logs ────────────────────────────────────────────
-- Left INTACT by default: job_log and data_quality_log are the run history and
-- are worth keeping across reruns. Uncomment for a truly clean slate.
-- TRUNCATE TABLE hpe_catalog.audit.job_log;
-- TRUNCATE TABLE hpe_catalog.audit.data_quality_log;

-- ── Re-create Gold ────────────────────────────────────────
-- Run data_model/03_gold_schema.sql after this to recreate the star schema
-- tables and the v_forecast_star view, then run the pipeline per subject.

-- ── Verify (all should return 0) ──────────────────────────
SELECT    'silver.o9_forecast_ref'        AS tbl, count(*) AS rows FROM hpe_catalog.silver.o9_forecast_ref
UNION ALL SELECT 'silver.o9_forecast_period_agg', count(*) FROM hpe_catalog.silver.o9_forecast_period_agg
UNION ALL SELECT 'silver.quarantine',             count(*) FROM hpe_catalog.silver.quarantine
ORDER BY tbl;
