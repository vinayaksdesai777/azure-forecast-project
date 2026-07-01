-- ============================================================
-- BRONZE LAYER - Delta Table Schema (Raw Data)
-- Raw ingestion layer with schema-on-read
-- Storage: abfss://bronze@<storage>.dfs.core.windows.net/
-- ============================================================

-- Create catalog and schema (Unity Catalog)
CREATE CATALOG IF NOT EXISTS hpe_catalog;
CREATE SCHEMA IF NOT EXISTS hpe_catalog.bronze;

-- Bronze table: stores raw CSV data as-is with audit columns
CREATE TABLE IF NOT EXISTS hpe_catalog.bronze.o9_forecast_raw (
    -- Source columns (all STRING in Bronze - schema-on-read)
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
    
    -- Frequency / classification column
    _frequency          STRING      COMMENT 'Data frequency: daily, weekly, monthly, quarterly',
    
    -- Operational / Audit columns
    _file_name          STRING      COMMENT 'Source file name',
    _ingestion_ts       TIMESTAMP   COMMENT 'Ingestion timestamp',
    _update_ts          TIMESTAMP   COMMENT 'Last update timestamp',
    _source_batch_nr    STRING      COMMENT 'Batch number extracted from filename',
    _load_job_nr        STRING      COMMENT 'Load job number (YYYYMMDDHHmmss)',
    _batch_id           STRING      COMMENT 'Pipeline batch identifier'
)
USING DELTA
COMMENT 'Bronze layer: raw o9 forecast data ingested from landing Parquet files'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'bronze'
);

-- ============================================================
-- Quarantine table: rows that failed PK null check at Bronze
-- All bronze source columns plus _dq_fail_reason
-- ============================================================
CREATE TABLE IF NOT EXISTS hpe_catalog.bronze.quarantine (
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
COMMENT 'Bronze quarantine: rows that failed DQ checks and were not written to the main Bronze table'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'bronze'
);
