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
COMMENT 'Bronze layer: raw o9 forecast data ingested from CSV files'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'bronze'
);
