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
    bronze_table        STRING                COMMENT 'Unused since the Bronze layer was removed; landing is the raw archive. Retained so existing config rows stay loadable.',
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
-- ── Full load (Jul 2020 - Jun 2025): 5-year historical baseline ────────────
--   Daily    (SAP HANA):   ~547,800 rows — 05_hana_daily_data.sql
--                            1,826 days x 300 rows/day
--   Quarterly(SAP HANA):    600,000 rows — 04_hana_quarterly_data.sql
--                            20 quarters x 30,000 rows/quarter
--   Weekly   (SQL Server): ~390,000 rows — 06_sqlserver_weekly_data.sql
--                            260 weeks x 1,500 rows/week
--   Monthly  (Salesforce): ~390,000 rows — salesforce/generate_full_load.py
--                            60 months x 5,000-8,000 rows/month
-- ── Incremental deltas (Jul 2025 - Jun 2026): 8-10k rows per ADF run ──────
--   Daily    (SAP HANA):      ~9,000 rows/run — 07_hana_daily_delta.sql
--   Quarterly(SAP HANA):      10,000 rows/run — 08_hana_quarterly_delta.sql
--   Weekly   (SQL Server):    ~8,500 rows/run — 09_sqlserver_weekly_delta.sql
--   Monthly  (Salesforce):    ~9,000 rows/run — salesforce/generate_monthly_delta.py
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
     'hpe_catalog.bronze.o9_forecast_raw', 'hpe_catalog.silver.o9_forecast_ref', 'hpe_catalog.gold.fact_forecast',
     'full', 'CHANGED_ON', NULL,
     'product_id,location_id,forecast_date,channel,customer_id', 'forecast_date', TRUE, 8, TRUE),

    -- SAP HANA Cloud: quarterly long-range (75k rows, 3 quarters: Q3-2025, Q4-2025, Q1-2026)
    ('o9_forecast_quarterly', 'SAP_HANA', 'SapHana', 'O9_SOURCE', 'FORECAST_QUARTERLY',
     'quarterly', 'o9/quarterly/',
     'hpe_catalog.bronze.o9_forecast_raw', 'hpe_catalog.silver.o9_forecast_ref', 'hpe_catalog.gold.fact_forecast',
     'full', 'CHANGED_ON', NULL,
     'product_id,location_id,forecast_date,channel,customer_id', 'forecast_date', TRUE, 4, TRUE),

    -- SQL Server on-prem: weekly aggregated (full load first, then incremental by modified_dt)
    ('o9_forecast_weekly', 'SQL_SERVER', 'SqlServer', 'dbo', 'Forecast',
     'weekly', 'o9/weekly/',
     'hpe_catalog.bronze.o9_forecast_raw', 'hpe_catalog.silver.o9_forecast_ref', 'hpe_catalog.gold.fact_forecast',
     'full', 'modified_dt', NULL,
     'product_id,location_id,forecast_date,channel,customer_id', 'forecast_date', TRUE, 8, TRUE),

    -- Salesforce Data Cloud: monthly strategic planning
    -- Source: Data Cloud DLO object (HPE_Forecast_Monthly__dll)
    -- Ingested via Data Cloud Ingestion API (salesforce/generate_full_load.py)
    -- ADF reads via SalesforceServiceCloud connector + SOQL on DLO
    -- Incremental by LastModifiedDate once full load is done
    -- Quarterly data is in SAP HANA Cloud (FORECAST_QUARTERLY) — see o9_forecast_quarterly row above
    ('o9_forecast_monthly', 'SALESFORCE', 'Salesforce', NULL, 'HPE_Forecast_Monthly__dll',
     'monthly', 'o9/monthly/',
     'hpe_catalog.bronze.o9_forecast_raw', 'hpe_catalog.silver.o9_forecast_ref', 'hpe_catalog.gold.fact_forecast',
     'full', 'LastModifiedDate', NULL,
     'product_id,location_id,forecast_date,channel,customer_id', 'forecast_date', TRUE, 4, TRUE);

-- ============================================================
-- Watermark update helper — call after each successful extract
-- to flip load_type to incremental and record the new high-watermark.
-- Usage: UPDATE hpe_catalog.audit.pipeline_config
--        SET last_watermark = '<new_ts>', load_type = 'incremental', updated_ts = current_timestamp()
--        WHERE data_subject = '<subject>';
-- The 00_update_watermark.py notebook does this automatically.
-- ============================================================
