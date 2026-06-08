-- ============================================================
-- GOLD LAYER - Delta Table Schema (Dimension / Business-Ready)
-- Business-ready dimension table (Star Schema)
-- Storage: abfss://gold@<storage>.dfs.core.windows.net/
-- ============================================================

CREATE SCHEMA IF NOT EXISTS hpe_catalog.gold;

-- ============================================================
-- Dimension Table (Star Schema - Fact/Dimension)
-- ============================================================
CREATE TABLE IF NOT EXISTS hpe_catalog.gold.o9_forecast_dmnsn (
    -- Business columns
    product_id          STRING      NOT NULL,
    location_id         STRING      NOT NULL,
    forecast_date       DATE        NOT NULL,
    forecast_qty        DECIMAL(18,4),
    revenue_amount      DECIMAL(18,2),
    customer_id         STRING,
    channel             STRING,
    category            STRING,
    sub_category        STRING,
    region              STRING,
    country             STRING,
    currency            STRING,
    uom                 STRING,
    
    -- Derived columns
    period              STRING      COMMENT 'Period partition (YYYY-MM-01 format)',
    _frequency          STRING      NOT NULL    COMMENT 'Data frequency: daily, weekly, monthly, quarterly',
    
    -- Audit columns
    _file_name          STRING,
    _ingestion_ts       TIMESTAMP,
    _update_ts          TIMESTAMP,
    _gold_load_ts       TIMESTAMP   COMMENT 'Gold layer load timestamp',
    _batch_id           STRING
)
USING DELTA
PARTITIONED BY (period, _frequency)
COMMENT 'Gold layer: business-ready o9 forecast dimension table partitioned by period and frequency'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'gold'
);

-- ============================================================
-- Aggregated Audit Table (KPI Metrics Summary)
-- KPI metrics summary for reporting and reconciliation
-- ============================================================
CREATE TABLE IF NOT EXISTS hpe_catalog.gold.o9_forecast_agg_audit (
    file_name           STRING      COMMENT 'Source file name or group key',
    no_of_records       BIGINT      COMMENT 'Number of records in the group',
    keyfigure           STRING      COMMENT 'KPI metric name (e.g., forecast_qty, revenue_amount)',
    total_qty_amount    DOUBLE      COMMENT 'Sum of the metric value',
    data_subject        STRING      COMMENT 'Data subject identifier',
    load_date           TIMESTAMP   COMMENT 'Load date timestamp',
    ins_gmt_ts          TIMESTAMP   COMMENT 'Insert GMT timestamp',
    ld_jb_nr            STRING      COMMENT 'Load job number'
)
USING DELTA
COMMENT 'Gold layer: aggregated audit/KPI summary table for o9 forecast'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'gold'
);

-- ============================================================
-- Dimension Tables (Star Schema supporting tables)
-- ============================================================

-- Product Dimension
CREATE TABLE IF NOT EXISTS hpe_catalog.gold.dim_product (
    product_id          STRING      NOT NULL,
    category            STRING,
    sub_category        STRING,
    product_name        STRING,
    is_active           BOOLEAN     DEFAULT true,
    effective_from      DATE,
    effective_to        DATE,
    _load_ts            TIMESTAMP
)
USING DELTA
COMMENT 'Product dimension table (SCD Type 2)';

-- Location Dimension
CREATE TABLE IF NOT EXISTS hpe_catalog.gold.dim_location (
    location_id         STRING      NOT NULL,
    region              STRING,
    country             STRING,
    city                STRING,
    is_active           BOOLEAN     DEFAULT true,
    effective_from      DATE,
    effective_to        DATE,
    _load_ts            TIMESTAMP
)
USING DELTA
COMMENT 'Location dimension table (SCD Type 2)';

-- Time Dimension
CREATE TABLE IF NOT EXISTS hpe_catalog.gold.dim_time (
    date_key            DATE        NOT NULL,
    year                INT,
    quarter             INT,
    month               INT,
    month_name          STRING,
    week_of_year        INT,
    day_of_week         INT,
    day_name            STRING,
    is_weekend          BOOLEAN,
    fiscal_year         INT,
    fiscal_quarter      INT,
    period              STRING      COMMENT 'YYYY-MM-01 format'
)
USING DELTA
COMMENT 'Time dimension table';
