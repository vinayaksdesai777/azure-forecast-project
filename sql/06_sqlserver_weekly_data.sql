-- ============================================================
-- SQL Server On-Prem — Weekly Forecast Data (Full Load Seed)
-- Schema: dbo | Table: Forecast
-- Run in SQL Server Management Studio (SSMS) or Azure Data Studio
-- FULL LOAD: ~390,000 rows — 260 weeks (Jul 2020 to Jun 2025) x 1,500 rows/week
-- After this: run 09_sqlserver_weekly_delta.sql for Jul 2025-Jun 2026 incremental (~8.5k rows/run)
-- ============================================================

USE HPE_SOURCE;
GO

-- Create table
-- DROP rather than TRUNCATE: an earlier run may have left a dbo.Forecast whose
-- columns predate period_type / fiscal_period. IF NOT EXISTS would skip the
-- CREATE, truncate that stale table, and then the INSERT below would fail on
-- columns that do not exist. Dropping makes the schema unconditional.
IF OBJECT_ID('dbo.Forecast', 'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.Forecast;
    PRINT 'Existing dbo.Forecast dropped.';
END
GO

CREATE TABLE dbo.Forecast (
    forecast_id      INT             IDENTITY(1,1) PRIMARY KEY,
    -- product_id is NULLable on purpose: the seed plants ~1% NULLs as dirty
    -- data for the Silver DQ checks to catch.
    product_id       NVARCHAR(50)    NULL,
    location_id      NVARCHAR(50)    NOT NULL,
    forecast_date    DATE            NOT NULL,
    forecast_qty     DECIMAL(12,2),
    revenue_amount   DECIMAL(16,2),
    customer_id      NVARCHAR(50),
    channel          NVARCHAR(50),
    category         NVARCHAR(50),
    sub_category     NVARCHAR(50),
    region           NVARCHAR(50),
    country          NVARCHAR(10),
    currency         NVARCHAR(10),
    uom              NVARCHAR(20)    DEFAULT 'UNIT',
    period_type      NVARCHAR(20)    DEFAULT 'WEEKLY',
    fiscal_period    NVARCHAR(20),
    modified_dt      DATETIME2       DEFAULT GETUTCDATE()    -- watermark column
);
GO

-- Business key uniqueness, as a FILTERED unique index rather than a UNIQUE
-- constraint. SQL Server treats NULLs as equal in a UNIQUE constraint, so the
-- second NULL-product_id row would collide with the first. The filter excludes
-- those rows from the index entirely, letting the dirty NULLs land while real
-- product/location/date triples stay unique.
CREATE UNIQUE INDEX uq_forecast_weekly
    ON dbo.Forecast (product_id, location_id, forecast_date)
    WHERE product_id IS NOT NULL;

CREATE INDEX ix_forecast_modified ON dbo.Forecast (modified_dt);
PRINT 'Table dbo.Forecast created.';
GO

-- ============================================================
-- Generate ~390,000 rows using a numbers CTE
-- 260 weeks (Jul 2020 — Jun 2025) x 1,500 rows/week
-- Dirty data: ~3% invalid categories, ~1.5% bad currencies, ~1% NULL product_id
-- ============================================================
WITH
weeks AS (
    SELECT TOP 260
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS wk_offset
    FROM sys.all_columns
),
series AS (
    SELECT TOP 1500
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i
    FROM sys.all_columns
),
raw AS (
    SELECT
        w.wk_offset,
        s.i,
        -- global row number, used only for attribute variation (qty, revenue,
        -- channel, dirty-data cadence) — never for the business key.
        w.wk_offset * 1500 + s.i + 1                                AS rn,
        -- Business-key indexes, derived from position WITHIN the week so that
        -- each week emits 1,500 distinct (product, location) pairs.
        -- Keying these to rn collided: product cycled on rn % 500 and location
        -- on (rn * 7) % 200, so the pair repeated every lcm(500,200) = 1000
        -- rows and ~500 rows per week duplicated an existing business key.
        -- 8 product blocks x 200 locations = 1,500 used of 1,600 possible.
        -- prod_key spreads the 8 per-week blocks across the full 500-product
        -- catalogue as weeks advance, so products recur over time rather than
        -- only the first 8 ever appearing. Computed here in `raw` (not in the
        -- enriched SELECT below) because SQL Server cannot reference a column
        -- alias from a sibling expression in the same SELECT list.
        (w.wk_offset * 8 + s.i / 200) % 500                         AS prod_key,
        s.i % 200                                                   AS loc_ix,
        -- Monday of each ISO week, starting 2020-07-06 (first Mon of Jul 2020)
        DATEADD(WEEK, w.wk_offset, CAST('2020-07-06' AS DATE))      AS forecast_date
    FROM weeks w CROSS JOIN series s
),
enriched AS (
    SELECT
        rn,
        prod_key,
        loc_ix,
        forecast_date,
        -- fiscal_period: YYYY-Www
        CAST(YEAR(forecast_date) AS NVARCHAR(4)) + N'-W'
            + RIGHT('0' + CAST(DATEPART(ISO_WEEK, forecast_date) AS NVARCHAR(2)), 2)
                                                                     AS fiscal_period,

        -- product_id: NULL for ~1% (rn % 100 = 99). These rows are excluded
        -- from the filtered unique index, so duplicate NULLs are fine.
        CASE WHEN rn % 100 = 99 THEN NULL
             ELSE N'HPE-PROD-' + RIGHT('000' + CAST(prod_key + 1 AS NVARCHAR(4)), 4)
        END                                                          AS product_id,

        N'LOC-' + RIGHT('00' + CAST(loc_ix + 1 AS NVARCHAR(3)), 3)
                                                                     AS location_id,

        N'CUST-' + RIGHT('0000' + CAST((rn * 13) % 50000 + 10000 AS NVARCHAR(5)), 5)
                                                                     AS customer_id,

        -- channel (5-way)
        CASE rn % 5
            WHEN 0 THEN N'DIRECT'
            WHEN 1 THEN N'ONLINE'
            WHEN 2 THEN N'PARTNER'
            WHEN 3 THEN N'DISTRIBUTOR'
            ELSE        N'VAR'
        END                                                          AS channel,

        -- base category (7 valid HPE categories)
        -- Derived from prod_key — the same expression that produces product_id
        -- — so a given product always carries the same category.
        -- Using rn % 7 directly made category a property of the row rather than
        -- the product: 500 and 7 are coprime, so every product cycled through
        -- all 7 categories and the Gold dimension churned a new SCD2 version on
        -- every load.
        CASE
            WHEN rn % 100 = 0 THEN N'HARDWARE'       -- dirty
            WHEN rn % 100 = 1 THEN N'UNKNOWN'         -- dirty
            WHEN rn % 100 = 2 THEN N'MISC'            -- dirty
            ELSE
                CASE prod_key % 7
                    WHEN 0 THEN N'SERVER'
                    WHEN 1 THEN N'STORAGE'
                    WHEN 2 THEN N'COMPUTE'
                    WHEN 3 THEN N'NETWORKING'
                    WHEN 4 THEN N'PRIVATE_CLOUD'
                    WHEN 5 THEN N'SUPERCOMPUTING'
                    ELSE        N'AI'
                END
        END                                                          AS category,

        -- sub_category likewise keyed to the product, and consistent with the
        -- category above (index 0 -> SERVER/PROLIANT, 1 -> STORAGE/PRIMERA, ...)
        CASE
            WHEN rn % 100 = 0 THEN N'LEGACY'
            WHEN rn % 100 = 1 THEN N'NA'
            WHEN rn % 100 = 2 THEN N'OTHER'
            ELSE
                CASE prod_key % 7
                    WHEN 0 THEN N'PROLIANT'
                    WHEN 1 THEN N'PRIMERA'
                    WHEN 2 THEN N'SYNERGY'
                    WHEN 3 THEN N'ARUBA'
                    WHEN 4 THEN N'GREENLAKE'
                    WHEN 5 THEN N'CRAY_EX'
                    ELSE        N'AI_CLUSTER'
                END
        END                                                          AS sub_category,

        -- region: keyed to loc_ix, the same index that produces location_id, so
        -- a location always sits in the same region.
        CASE loc_ix % 4
            WHEN 0 THEN N'NORTH_AMERICA'
            WHEN 1 THEN N'EMEA'
            WHEN 2 THEN N'APJ'
            ELSE        N'LATAM'
        END                                                          AS region,

        -- country
        -- Keyed to loc_ix — the same index as location_id — so a location
        -- always sits in the same country. The inner % 2 / % 3 terms key to
        -- loc_ix too; keying them to rn left half the locations spanning two
        -- countries and churned a new dim_location version on every load.
        CASE loc_ix % 4
            WHEN 0 THEN CASE loc_ix % 2 WHEN 0 THEN N'US' ELSE N'CA' END
            WHEN 1 THEN CASE loc_ix % 3 WHEN 0 THEN N'DE' WHEN 1 THEN N'FR' ELSE N'UK' END
            WHEN 2 THEN CASE loc_ix % 3 WHEN 0 THEN N'JP' WHEN 1 THEN N'IN' ELSE N'SG' END
            ELSE        CASE loc_ix % 2 WHEN 0 THEN N'BR' ELSE N'MX' END
        END                                                          AS country,

        -- currency (~1.5% bad = XYZ every 67th row), otherwise following the
        -- location's region so the two stay consistent.
        CASE WHEN rn % 67 = 0 THEN N'XYZ'
             ELSE
                CASE loc_ix % 4
                    WHEN 0 THEN N'USD'
                    WHEN 1 THEN N'EUR'
                    WHEN 2 THEN N'SGD'
                    ELSE        N'BRL'
                END
        END                                                          AS currency,

        -- weekly quantities (larger than daily, smaller than quarterly)
        CAST(rn * 11 % 9800 + 100 AS DECIMAL(12,2))                 AS forecast_qty,
        CAST((rn * 11 % 9800 + 100) * (rn * 31 % 450 + 50) AS DECIMAL(16,2))
                                                                     AS revenue_amount
    FROM raw
)
INSERT INTO dbo.Forecast
    (product_id, location_id, forecast_date, forecast_qty, revenue_amount,
     customer_id, channel, category, sub_category, region, country,
     currency, uom, period_type, fiscal_period, modified_dt)
SELECT
    product_id, location_id, forecast_date, forecast_qty, revenue_amount,
    customer_id, channel, category, sub_category, region, country,
    currency, N'UNIT', N'WEEKLY', fiscal_period,
    -- modified_dt must sit on the same timeline as forecast_date, not on the
    -- wall clock. Stamping GETUTCDATE() here put the full load in the seeding
    -- month (Aug 2026) while 09_sqlserver_weekly_delta stamps its rows with
    -- their forecast week (Jan-Jun 2026) — so the delta looked OLDER than the
    -- full load, and an incremental run filtering modified_dt > watermark
    -- returned nothing at all. Derive it from forecast_date instead, staggered
    -- within the day so the watermark still advances row to row.
    DATEADD(SECOND, rn % 86400, CAST(forecast_date AS DATETIME2))
FROM enriched;
-- No WHERE clause: the NULL-product_id rows are meant to land as dirty data,
-- and the filtered unique index lets them. The previous
-- "WHERE product_id IS NOT NULL OR 1 = 1" was a no-op — it always evaluates
-- true — so it never filtered anything despite the comment claiming otherwise.
GO

-- Verify
SELECT COUNT(*)        AS total_rows  FROM dbo.Forecast;   -- expect 390,000
SELECT COUNT(*)        AS null_product_rows FROM dbo.Forecast WHERE product_id IS NULL;

-- Business key must be unique among non-NULL products (expect zero rows)
SELECT product_id, location_id, forecast_date, COUNT(*) AS dupes
FROM dbo.Forecast
WHERE product_id IS NOT NULL
GROUP BY product_id, location_id, forecast_date
HAVING COUNT(*) > 1;
SELECT fiscal_period,  COUNT(*) AS row_count FROM dbo.Forecast GROUP BY fiscal_period ORDER BY fiscal_period;
SELECT category,       COUNT(*) AS row_count FROM dbo.Forecast GROUP BY category       ORDER BY row_count DESC;
SELECT MIN(modified_dt) AS oldest_watermark, MAX(modified_dt) AS latest_watermark FROM dbo.Forecast;
GO
