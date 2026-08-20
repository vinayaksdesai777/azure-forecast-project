-- ============================================================
-- GOLD LAYER - Delta Table Schema (Star Schema)
-- Storage: abfss://gold@<storage>.dfs.core.windows.net/
--
-- Design notes
--   * Conformed dimensions (product, location, time) + one periodic snapshot
--     fact. The fact carries surrogate keys, not descriptive attributes, so a
--     dimension attribute lives in exactly one place.
--   * dim_product / dim_location are SCD Type 2: each version gets its own
--     surrogate key, so a fact row keeps pointing at the version that was
--     current when it loaded.
--   * Surrogate keys are deterministic sha2(natural_key || effective_from)
--     rather than IDENTITY, so they survive a full rebuild of the warehouse
--     and stay stable across environments.
--   * The fact is a PERIODIC SNAPSHOT: the grain is one row per
--     product / location / date / channel / customer / frequency, and a
--     restated forecast REPLACES the prior value for that grain. Loads use
--     replaceWhere on _frequency so each data subject rebuilds only its own
--     slice of a table shared by all four subjects.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS hpe_catalog.gold;

-- ============================================================
-- Dimension: Product (SCD Type 2)
-- ============================================================
CREATE TABLE IF NOT EXISTS hpe_catalog.gold.dim_product (
    product_sk          STRING      NOT NULL  COMMENT 'Surrogate key: sha2(product_id || effective_from)',
    product_id          STRING      NOT NULL  COMMENT 'Natural/business key from source',
    category            STRING,
    sub_category        STRING,
    product_name        STRING,
    is_active           BOOLEAN               COMMENT 'TRUE for the current version',
    effective_from      DATE,
    effective_to        DATE                  COMMENT 'NULL while current',
    _load_ts            TIMESTAMP
)
USING DELTA
COMMENT 'Product dimension (SCD Type 2, surrogate-keyed)'
TBLPROPERTIES ('quality' = 'gold');

-- ============================================================
-- Dimension: Location (SCD Type 2)
-- ============================================================
CREATE TABLE IF NOT EXISTS hpe_catalog.gold.dim_location (
    location_sk         STRING      NOT NULL  COMMENT 'Surrogate key: sha2(location_id || effective_from)',
    location_id         STRING      NOT NULL  COMMENT 'Natural/business key from source',
    region              STRING,
    country             STRING,
    city                STRING,
    is_active           BOOLEAN               COMMENT 'TRUE for the current version',
    effective_from      DATE,
    effective_to        DATE                  COMMENT 'NULL while current',
    _load_ts            TIMESTAMP
)
USING DELTA
COMMENT 'Location dimension (SCD Type 2, surrogate-keyed)'
TBLPROPERTIES ('quality' = 'gold');

-- ============================================================
-- Dimension: Time
-- date_key is a natural date key — stable, meaningful, and never versioned,
-- so it needs no surrogate.
-- ============================================================
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
COMMENT 'Time dimension'
TBLPROPERTIES ('quality' = 'gold');

-- ============================================================
-- Fact: Forecast (periodic snapshot)
--
-- Grain: one row per product / location / forecast_date / channel /
--        customer / frequency.
-- Measures (forecast_qty, revenue_amount) are additive across every dimension.
-- Descriptive attributes deliberately live in the dimensions only.
-- ============================================================
CREATE TABLE IF NOT EXISTS hpe_catalog.gold.fact_forecast (
    -- Dimension foreign keys
    product_sk          STRING      NOT NULL  COMMENT 'FK -> dim_product.product_sk',
    location_sk         STRING      NOT NULL  COMMENT 'FK -> dim_location.location_sk',
    date_key            DATE        NOT NULL  COMMENT 'FK -> dim_time.date_key',

    -- Degenerate dimensions: no attributes of their own, so they stay on the fact
    channel             STRING,
    customer_id         STRING,

    -- Natural keys retained for lineage and reconciliation against Silver
    product_id          STRING      NOT NULL,
    location_id         STRING      NOT NULL,
    forecast_date       DATE        NOT NULL,

    -- Additive measures
    forecast_qty        DECIMAL(18,4),
    revenue_amount      DECIMAL(18,2),
    currency            STRING      COMMENT 'Unit of the revenue_amount measure',
    uom                 STRING      COMMENT 'Unit of the forecast_qty measure',

    -- Partition / audit
    period              STRING      COMMENT 'Period partition (YYYY-MM-01)',
    _frequency          STRING      NOT NULL  COMMENT 'daily, weekly, monthly, quarterly',
    _gold_load_ts       TIMESTAMP,
    _batch_id           STRING
)
USING DELTA
PARTITIONED BY (period, _frequency)
COMMENT 'Gold fact: forecast periodic snapshot, surrogate-keyed to conformed dimensions'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'quality' = 'gold'
);

-- ============================================================
-- Aggregated Audit Table (KPI Metrics Summary)
-- Written per data subject by 04_aggregated_audit.
-- ============================================================
CREATE TABLE IF NOT EXISTS hpe_catalog.gold.o9_forecast_agg_audit (
    file_name           STRING      COMMENT 'Source file name or group key',
    no_of_records       BIGINT      COMMENT 'Number of records in the group',
    keyfigure           STRING      COMMENT 'KPI metric name (forecast_qty, revenue_amount)',
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
-- Reporting convenience view — rejoins the star for ad-hoc queries.
-- Every dimension join is filtered to the current version, which is what
-- keeps an SCD2 dimension from double-counting measures.
-- ============================================================
CREATE OR REPLACE VIEW hpe_catalog.gold.v_forecast_star AS
SELECT
    f.forecast_date,
    f.period,
    f._frequency,
    f.channel,
    f.customer_id,
    p.product_id,
    p.category,
    p.sub_category,
    l.location_id,
    l.region,
    l.country,
    t.year,
    t.quarter,
    t.month_name,
    f.forecast_qty,
    f.revenue_amount,
    f.currency,
    f.uom,
    f._gold_load_ts,
    f._batch_id
FROM       hpe_catalog.gold.fact_forecast f
-- Join the ACTIVE dimension version only. resolve_dim_sk already pinned each
-- fact row to the active surrogate key, so re-expanding across every version
-- here double-counts: a product that churned category has expired versions
-- alongside the active one, and product_sk is not unique across them —
-- sha2(product_id || effective_from) collides for two versions created on the
-- same day, which is what happens when several subjects load in one session.
-- Without this filter the star fanned out to 4,030,130 rows against 1,972,706
-- facts, while dim_location (which never churns) stayed exact.
LEFT JOIN  hpe_catalog.gold.dim_product   p ON f.product_sk  = p.product_sk
                                           AND p.is_active = true
LEFT JOIN  hpe_catalog.gold.dim_location  l ON f.location_sk = l.location_sk
                                           AND l.is_active = true
LEFT JOIN  hpe_catalog.gold.dim_time      t ON f.date_key    = t.date_key;
