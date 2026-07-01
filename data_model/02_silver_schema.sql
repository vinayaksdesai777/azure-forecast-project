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
    _batch_id           STRING      COMMENT 'Pipeline batch identifier'
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
