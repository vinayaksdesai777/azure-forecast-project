-- ============================================================
-- AUDIT LAYER — Pipeline Config Tables (Unity Catalog Delta)
-- Replaces Azure SQL audit.pipeline_metadata and
-- audit.source_extract_metadata entirely.
-- ADF reads these via the 00_get_metadata notebook, not JDBC.
-- Run AFTER 04_audit_schema.sql.
-- ============================================================

-- ============================================================
-- pipeline_config: per-data-subject runtime configuration
-- Equivalent to old Azure SQL audit.pipeline_metadata
-- ============================================================
CREATE TABLE IF NOT EXISTS hpe_catalog.audit.pipeline_config (
    data_subject        STRING      NOT NULL  COMMENT 'o9_forecast_daily / weekly / monthly / quarterly',
    source_system       STRING      NOT NULL  COMMENT 'SAP_HANA / SQL_SERVER / SALESFORCE',
    connector           STRING      NOT NULL  COMMENT 'SapHana / SqlServer / Salesforce (Switch key in ADF)',
    source_schema       STRING                COMMENT 'O9_SOURCE / dbo / NULL for Salesforce',
    source_object       STRING      NOT NULL  COMMENT 'Table, view, or SOQL object name',
    frequency           STRING      NOT NULL  COMMENT 'daily / weekly / monthly / quarterly',
    landing_path        STRING      NOT NULL  COMMENT 'Folder under landing container (e.g. o9/daily/)',
    bronze_table        STRING      NOT NULL  COMMENT 'hpe_catalog.bronze.<table>',
    silver_table        STRING      NOT NULL  COMMENT 'hpe_catalog.silver.<table>',
    gold_table          STRING      NOT NULL  COMMENT 'hpe_catalog.gold.<table>',
    load_type           STRING      NOT NULL  COMMENT 'full / incremental',
    watermark_column    STRING                COMMENT 'Column used for incremental high-watermark',
    last_watermark      STRING                COMMENT 'Last successfully extracted watermark value',
    null_check_columns  STRING                COMMENT 'Comma-separated PK columns for Bronze null check',
    partition_column    STRING                COMMENT 'Column used for period derivation in Gold',
    apply_concat        BOOLEAN     DEFAULT TRUE COMMENT 'Derive period column from partition_column',
    num_partitions      INT         DEFAULT 8 COMMENT 'Spark repartition target',
    is_active           BOOLEAN     DEFAULT TRUE COMMENT 'FALSE = skip this subject in the pipeline',
    created_ts          TIMESTAMP   DEFAULT current_timestamp() COMMENT 'Row creation time',
    updated_ts          TIMESTAMP   DEFAULT current_timestamp() COMMENT 'Last update time'
)
USING DELTA
COMMENT 'Pipeline runtime config — read by 00_get_metadata.py; replaces Azure SQL audit.pipeline_metadata'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'audit'
);

-- ============================================================
-- Seed: one row per data subject
-- First loads are FULL (load_type='full', last_watermark=NULL).
-- After first run, update load_type='incremental' and set watermark.
-- Data range: Jan 2025 – Jun 2026 for all sources.
-- Row counts for first full load:
--   Daily (SAP HANA):    ~199,290 rows (546 days x 365 pairs/day)
--   Quarterly (SAP HANA): 150,000 rows (6 quarters x 25,000)
--   Weekly (SQL Server): ~112,476 rows (78 weeks x 1,442 pairs/week)
--   Monthly (Salesforce):  84,000 rows (9 months, storage-capped)
-- ============================================================
INSERT INTO hpe_catalog.audit.pipeline_config
    (data_subject, source_system, connector, source_schema, source_object,
     frequency, landing_path,
     bronze_table, silver_table, gold_table,
     load_type, watermark_column, last_watermark,
     null_check_columns, partition_column, apply_concat, num_partitions, is_active)
VALUES
    -- SAP HANA Cloud: daily granular SKU-level (full load first, then incremental by CHANGED_ON)
    ('o9_forecast_daily', 'SAP_HANA', 'SapHana', 'O9_SOURCE', 'FORECAST_VIEW',
     'daily', 'o9/daily/',
     'hpe_catalog.bronze.o9_forecast_raw', 'hpe_catalog.silver.o9_forecast_ref', 'hpe_catalog.gold.o9_forecast_dmnsn',
     'full', 'CHANGED_ON', NULL,
     'product_id,location_id,forecast_date', 'forecast_date', TRUE, 8, TRUE),

    -- SAP HANA Cloud: quarterly long-range (75k rows, 3 quarters: Q3-2025, Q4-2025, Q1-2026)
    ('o9_forecast_quarterly', 'SAP_HANA', 'SapHana', 'O9_SOURCE', 'FORECAST_QUARTERLY',
     'quarterly', 'o9/quarterly/',
     'hpe_catalog.bronze.o9_forecast_raw', 'hpe_catalog.silver.o9_forecast_ref', 'hpe_catalog.gold.o9_forecast_dmnsn',
     'full', 'CHANGED_ON', NULL,
     'product_id,location_id,forecast_date', 'forecast_date', TRUE, 4, TRUE),

    -- SQL Server on-prem: weekly aggregated (full load first, then incremental by modified_dt)
    ('o9_forecast_weekly', 'SQL_SERVER', 'SqlServer', 'dbo', 'Forecast',
     'weekly', 'o9/weekly/',
     'hpe_catalog.bronze.o9_forecast_raw', 'hpe_catalog.silver.o9_forecast_ref', 'hpe_catalog.gold.o9_forecast_dmnsn',
     'full', 'modified_dt', NULL,
     'product_id,location_id,forecast_date', 'forecast_date', TRUE, 8, TRUE),

    -- Salesforce: monthly strategic planning (always full load — no watermark on Forecast__c)
    -- Quarterly data is in SAP HANA Cloud (FORECAST_QUARTERLY) — see o9_forecast_quarterly row above
    ('o9_forecast_monthly', 'SALESFORCE', 'Salesforce', NULL, 'Forecast__c',
     'monthly', 'o9/monthly/',
     'hpe_catalog.bronze.o9_forecast_raw', 'hpe_catalog.silver.o9_forecast_ref', 'hpe_catalog.gold.o9_forecast_dmnsn',
     'full', NULL, NULL,
     'product_id,location_id,forecast_date', 'forecast_date', TRUE, 4, TRUE);

-- ============================================================
-- Watermark update helper — call after each successful extract
-- to flip load_type to incremental and record the new high-watermark.
-- Usage: UPDATE hpe_catalog.audit.pipeline_config
--        SET last_watermark = '<new_ts>', load_type = 'incremental', updated_ts = current_timestamp()
--        WHERE data_subject = '<subject>';
-- The 00_update_watermark.py notebook does this automatically.
-- ============================================================
