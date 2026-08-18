-- ============================================================
-- SILVER LAYER - Delta Table Schema (Cleansed/Reference)
-- Cleansed and validated reference layer
-- Storage: abfss://silver@<storage>.dfs.core.windows.net/
-- ============================================================

CREATE SCHEMA IF NOT EXISTS hpe_catalog.silver;

-- Silver table: validated, typed, deduplicated data
CREATE TABLE IF NOT EXISTS hpe_catalog.silver.o9_forecast_ref (
    -- Business columns (properly typed in Silver)
    product_id          STRING      NOT NULL    COMMENT 'Product identifier (PK)',
    location_id         STRING      NOT NULL    COMMENT 'Location identifier (PK)',
    forecast_date       DATE        NOT NULL    COMMENT 'Forecast date (PK)',
    forecast_qty        DECIMAL(18,4)           COMMENT 'Forecasted quantity',
    revenue_amount      DECIMAL(18,2)           COMMENT 'Revenue amount',
    customer_id         STRING                  COMMENT 'Customer identifier',
    channel             STRING                  COMMENT 'Sales channel',
    category            STRING                  COMMENT 'Product category',
    sub_category        STRING                  COMMENT 'Product sub-category',
    region              STRING                  COMMENT 'Geographic region',
    country             STRING                  COMMENT 'Country code',
    currency            STRING                  COMMENT 'Currency code (ISO)',
    uom                 STRING                  COMMENT 'Unit of measure',
    
    -- Frequency / classification column
    _frequency          STRING      NOT NULL    COMMENT 'Data frequency: daily, weekly, monthly, quarterly',

    -- SCD Type 2 tracking columns
    effective_from      DATE                    COMMENT 'Date this version became active',
    effective_to        DATE                    COMMENT 'Date this version expired (NULL = current)',
    is_active           BOOLEAN     DEFAULT TRUE COMMENT 'TRUE = current active version',

    -- Operational / Audit columns
    _file_name          STRING      COMMENT 'Source file name',
    _ingestion_ts       TIMESTAMP   COMMENT 'Original ingestion timestamp',
    _update_ts          TIMESTAMP   COMMENT 'Update timestamp',
    _ingestion_date     DATE        COMMENT 'Ingestion date (partition key)',
    _silver_load_ts     TIMESTAMP   COMMENT 'Silver layer load timestamp',
    _load_job_nr        STRING      COMMENT 'Extract job number, set by AddAuditColumnsTransform',
    _batch_id           STRING      COMMENT 'Pipeline batch identifier',

    -- Source period labels, kept underscore-prefixed so they stay out of the
    -- business columns above. 01_landing_to_silver maps the Salesforce
    -- period_type__c / fiscal_period__c onto these; the HANA and SQL Server
    -- extracts supply the same two fields. Gold does not read them, but the
    -- SCD2 merge resolves every target column, so they must be declared.
    _period_type        STRING      COMMENT 'Source period type: DAILY / WEEKLY / MONTHLY / QUARTERLY',
    _fiscal_period      STRING      COMMENT 'Source fiscal period label, e.g. 2025-Q2 or 2025-W14'
)
USING DELTA
PARTITIONED BY (_ingestion_date)
COMMENT 'Silver layer: cleansed and validated o9 forecast reference data'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.dataSkippingNumIndexedCols' = '8',
    'quality' = 'silver'
);

-- Optimize for common query patterns
-- OPTIMIZE hpe_catalog.silver.o9_forecast_ref ZORDER BY (product_id, location_id, forecast_date);

-- ============================================================
-- Period aggregation table: pre-rolled-up metrics by period
-- Written by 02_bronze_to_silver.py and consumed by Gold layer
-- ============================================================
CREATE TABLE IF NOT EXISTS hpe_catalog.silver.o9_forecast_period_agg (
    period              STRING      NOT NULL    COMMENT 'First day of month (yyyy-MM-01)',
    _frequency          STRING      NOT NULL    COMMENT 'daily, weekly, monthly, quarterly',
    category            STRING                  COMMENT 'Product category',
    region              STRING                  COMMENT 'Geographic region',
    source_system       STRING                  COMMENT 'SAP_HANA / SQL_SERVER / SALESFORCE',
    total_forecast_qty  DECIMAL(18,4)           COMMENT 'Sum of forecast_qty for the period',
    total_revenue_amount DECIMAL(18,2)          COMMENT 'Sum of revenue_amount for the period',
    record_count        BIGINT                  COMMENT 'Number of detail rows aggregated',
    _batch_id           STRING                  COMMENT 'Pipeline batch identifier',
    _agg_load_ts        TIMESTAMP               COMMENT 'When this aggregate row was written'
)
USING DELTA
COMMENT 'Silver aggregation: period-level rolled-up forecast KPIs for Gold consumption'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'silver'
);

-- ============================================================
-- Silver quarantine — rows that failed structural DQ (null PK, duplicates).
-- Written by 01_landing_to_silver BEFORE the type cast, so values are kept in
-- their original string form for investigation.
-- ============================================================
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
)
USING DELTA
COMMENT 'Silver quarantine: rows that failed structural DQ and were not merged into Silver'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'silver'
);
