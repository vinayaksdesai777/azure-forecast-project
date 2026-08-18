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

-- Qualify every name below, so the script does not depend on the session's
-- current catalog/schema.
USE CATALOG hpe_catalog;

-- ── Silver ────────────────────────────────────────────────
-- Shared by all 4 subjects, separated by the _frequency partition.
TRUNCATE TABLE hpe_catalog.silver.o9_forecast_ref;
TRUNCATE TABLE hpe_catalog.silver.o9_forecast_period_agg;

-- Drop columns left behind by earlier schema versions. TRUNCATE keeps the
-- schema, so a column added by a previous run's mergeSchema:true write
-- survives a reset. apply_scd2_merge uses whenNotMatchedInsertAll(), which
-- resolves every target column against the incoming DataFrame, so one stale
-- column fails the whole merge with DELTA_MERGE_UNRESOLVED_EXPRESSION.
--
-- These four are raw source column names that leaked in before the
-- harmonization map in 01_landing_to_silver existed: changed_on is HANA's
-- CHANGED_ON, modified_dt is the SQL Server watermark, and period_type /
-- fiscal_period are the unprefixed forms of _period_type / _fiscal_period.
-- Nothing in databricks/ reads any of them.
--
-- DROP COLUMN needs Delta column mapping. If this errors with
-- DELTA_UNSUPPORTED_DROP_COLUMN, the table is empty after the TRUNCATE above,
-- so the simpler route is to DROP TABLE it and re-run
-- data_model/02_silver_schema.sql.
ALTER TABLE hpe_catalog.silver.o9_forecast_ref DROP COLUMN IF EXISTS period_type;
ALTER TABLE hpe_catalog.silver.o9_forecast_ref DROP COLUMN IF EXISTS fiscal_period;
ALTER TABLE hpe_catalog.silver.o9_forecast_ref DROP COLUMN IF EXISTS changed_on;
ALTER TABLE hpe_catalog.silver.o9_forecast_ref DROP COLUMN IF EXISTS modified_dt;

-- Structural-DQ rejects. Also append-only, so it accumulates across runs.
-- CREATE ... IF NOT EXISTS first: run_dq_checks creates this table lazily on
-- the first quarantined row, so on an environment that has never rejected a
-- row it does not exist yet and a bare TRUNCATE fails with
-- TABLE_OR_VIEW_NOT_FOUND. Creating it empty makes the reset idempotent.
-- Definition kept in step with data_model/02_silver_schema.sql.
CREATE TABLE IF NOT EXISTS hpe_catalog.silver.quarantine (
    product_id          STRING,
    location_id         STRING,
    forecast_date       STRING,
    forecast_qty        STRING,
    revenue_amount      STRING,
    customer_id         STRING,
    channel             STRING,
    category            STRING,
    sub_category        STRING,
    region              STRING,
    country             STRING,
    currency            STRING,
    uom                 STRING,
    _frequency          STRING,
    _file_name          STRING,
    _ingestion_ts       TIMESTAMP,
    _update_ts          TIMESTAMP,
    _load_job_nr        STRING,
    _batch_id           STRING,
    _dq_fail_reason     STRING      COMMENT 'Reason row was quarantined: NULL_PK, DEDUP, etc.'
) USING DELTA;

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
