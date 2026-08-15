-- ============================================================
-- Drop the Bronze layer.
--
-- Landing already holds untransformed source Parquet and is copied to the
-- archive container after every load, so it serves the raw-archive role the
-- medallion pattern assigns to Bronze: reprocessing never has to re-read the
-- source systems. The Bronze Delta table was an extra hop that added a copy
-- without adding a guarantee.
--
-- 01_ingest_to_bronze and 02_bronze_to_silver are replaced by the single
-- 01_landing_to_silver notebook, which harmonizes column names, runs structural
-- DQ on raw strings, type casts, runs domain DQ, and merges into Silver.
--
-- Run this AFTER a successful Silver reload, so Bronze is no longer the only
-- copy of anything. Landing and archive are untouched.
-- ============================================================

-- ── Confirm Silver is populated before dropping Bronze ────
-- Expect one row per loaded frequency with a non-zero count.
SELECT _frequency, count(*) AS rows
FROM   hpe_catalog.silver.o9_forecast_ref
GROUP  BY _frequency
ORDER  BY _frequency;

-- ── Drop Bronze ───────────────────────────────────────────
DROP TABLE IF EXISTS hpe_catalog.bronze.o9_forecast_raw;
DROP TABLE IF EXISTS hpe_catalog.bronze.quarantine;
DROP SCHEMA IF EXISTS hpe_catalog.bronze CASCADE;

-- ── Clear the now-unused config column ────────────────────
-- bronze_table is retained in the pipeline_config schema (nullable) so existing
-- rows stay loadable, but nothing reads it any more.
UPDATE hpe_catalog.audit.pipeline_config
SET    bronze_table = NULL
WHERE  bronze_table IS NOT NULL;

-- ── Verify ────────────────────────────────────────────────
SELECT data_subject, frequency, silver_table, gold_table
FROM   hpe_catalog.audit.pipeline_config
WHERE  is_active = true
ORDER  BY data_subject;

-- NOTE: the ADLS bronze/ container still holds the dropped tables' Parquet
-- files. Delete that container manually once you are satisfied with the
-- reload — DROP TABLE removes the Unity Catalog registration and, for managed
-- tables, the files; external tables leave the storage in place.
