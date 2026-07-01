-- ============================================================
-- AUDIT LAYER - Unity Catalog Delta Tables
-- Job tracking and data quality logs.
-- Replaces Azure SQL audit.pipeline_audit for job tracking.
-- Azure SQL keeps pipeline_metadata + source_extract_metadata
-- (config only — needed by ADF Lookup activities).
-- ============================================================

CREATE SCHEMA IF NOT EXISTS hpe_catalog.audit;

-- ============================================================
-- Job Log: one row per notebook run per layer
-- Tracks batch_id, insert_time, records inserted/updated,
-- status, and layer — the core job tracking table.
-- ============================================================
CREATE TABLE IF NOT EXISTS hpe_catalog.audit.job_log (
    batch_id          STRING      NOT NULL  COMMENT 'UUID batch identifier linking all layers in one run',
    insert_time       TIMESTAMP   NOT NULL  COMMENT 'When this row was written',
    layer             STRING                COMMENT 'bronze / silver / gold / agg_audit',
    status            STRING                COMMENT 'SUCCESS / FAILED',
    records_inserted  BIGINT                COMMENT 'New rows written to target table',
    records_updated   BIGINT                COMMENT 'Rows updated via SCD2 merge (Silver/Gold)',
    error_records     BIGINT                COMMENT 'Rows routed to quarantine or flagged',
    source_system     STRING                COMMENT 'SAP_HANA / SQL_SERVER / SALESFORCE',
    data_subject      STRING                COMMENT 'o9_forecast_daily / weekly / monthly / quarterly',
    object_name       STRING                COMMENT 'Target Delta table name',
    error_message     STRING                COMMENT 'Exception message on failure (NULL on success)',
    run_id            STRING                COMMENT 'ADF pipeline run ID'
)
USING DELTA
COMMENT 'Unified job tracking log — one row per layer per pipeline run'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'audit'
);

-- ============================================================
-- Data Quality Log: one row per DQ check per run
-- Records check type, pass/fail counts per batch.
-- ============================================================
CREATE TABLE IF NOT EXISTS hpe_catalog.audit.data_quality_log (
    batch_id          STRING      NOT NULL  COMMENT 'Links back to job_log.batch_id',
    insert_time       TIMESTAMP   NOT NULL  COMMENT 'When this check was recorded',
    layer             STRING                COMMENT 'bronze / silver',
    table_name        STRING                COMMENT 'Table the check ran against',
    check_type        STRING                COMMENT 'NULL_PK / DEDUP / CURRENCY_VALIDATION / CATEGORY_VALIDATION / TYPE_CAST',
    column_name       STRING                COMMENT 'Column(s) the check targeted',
    records_checked   BIGINT                COMMENT 'Total rows evaluated',
    records_passed    BIGINT                COMMENT 'Rows that passed the check',
    records_failed    BIGINT                COMMENT 'Rows that failed the check',
    data_subject      STRING                COMMENT 'o9_forecast_daily / weekly / monthly / quarterly'
)
USING DELTA
COMMENT 'Data quality check results — one row per check per run'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'audit'
);
