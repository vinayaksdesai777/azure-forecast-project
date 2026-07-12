-- ============================================================
-- SQL Server On-Prem — Weekly Forecast Data (Full Load Seed)
-- Schema: dbo | Table: Forecast
-- Run in SQL Server Management Studio (SSMS) or Azure Data Studio
-- Target: ~112,476 rows — 78 weeks (Jan 2025 to Jun 2026) x 1,442 pairs/week
-- ============================================================

USE ForecastDB;   -- change to your actual database name
GO

-- Create table
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Forecast' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.Forecast (
        forecast_id      INT             IDENTITY(1,1) PRIMARY KEY,
        product_id       NVARCHAR(50)    NOT NULL,
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
        modified_dt      DATETIME2       DEFAULT GETUTCDATE(),   -- watermark column
        CONSTRAINT uq_forecast_weekly UNIQUE (product_id, location_id, forecast_date)
    );
    CREATE INDEX ix_forecast_modified ON dbo.Forecast (modified_dt);
    PRINT 'Table dbo.Forecast created.';
END
ELSE
    PRINT 'Table dbo.Forecast already exists — truncating for fresh seed.';

TRUNCATE TABLE dbo.Forecast;
GO

-- ============================================================
-- Generate ~112,476 rows using a numbers CTE
-- 78 weeks (Jan 2025 — Jun 2026) x 1,442 rows/week
-- Dirty data: ~3% invalid categories, ~1.5% bad currencies, ~1% NULL product_id
-- ============================================================
WITH
weeks AS (
    SELECT TOP 78
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS wk_offset
    FROM sys.all_columns
),
series AS (
    SELECT TOP 1442
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i
    FROM sys.all_columns
),
raw AS (
    SELECT
        w.wk_offset,
        s.i,
        -- global row number for hash-based variation
        w.wk_offset * 1442 + s.i + 1                                AS rn,
        -- Monday of each ISO week, starting 2025-01-06 (first Mon of Jan 2025)
        DATEADD(WEEK, w.wk_offset, CAST('2025-01-06' AS DATE))      AS forecast_date
    FROM weeks w CROSS JOIN series s
),
enriched AS (
    SELECT
        rn,
        forecast_date,
        -- fiscal_period: YYYY-Www
        CAST(YEAR(forecast_date) AS NVARCHAR(4)) + N'-W'
            + RIGHT('0' + CAST(DATEPART(ISO_WEEK, forecast_date) AS NVARCHAR(2)), 2)
                                                                     AS fiscal_period,

        -- product_id: NULL for ~1% (rn % 100 = 99)
        CASE WHEN rn % 100 = 99 THEN NULL
             ELSE N'HPE-PROD-' + RIGHT('000' + CAST(rn % 500 + 1 AS NVARCHAR(4)), 4)
        END                                                          AS product_id,

        N'LOC-' + RIGHT('00' + CAST((rn * 7) % 200 + 1 AS NVARCHAR(3)), 3)
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
        CASE
            WHEN rn % 100 = 0 THEN N'HARDWARE'       -- dirty
            WHEN rn % 100 = 1 THEN N'UNKNOWN'         -- dirty
            WHEN rn % 100 = 2 THEN N'MISC'            -- dirty
            ELSE
                CASE rn % 7
                    WHEN 0 THEN N'SERVER'
                    WHEN 1 THEN N'STORAGE'
                    WHEN 2 THEN N'COMPUTE'
                    WHEN 3 THEN N'NETWORKING'
                    WHEN 4 THEN N'PRIVATE_CLOUD'
                    WHEN 5 THEN N'SUPERCOMPUTING'
                    ELSE        N'AI'
                END
        END                                                          AS category,

        CASE
            WHEN rn % 100 = 0 THEN N'LEGACY'
            WHEN rn % 100 = 1 THEN N'NA'
            WHEN rn % 100 = 2 THEN N'OTHER'
            ELSE
                CASE rn % 7
                    WHEN 0 THEN N'PROLIANT'
                    WHEN 1 THEN N'PRIMERA'
                    WHEN 2 THEN N'SYNERGY'
                    WHEN 3 THEN N'ARUBA'
                    WHEN 4 THEN N'GREENLAKE'
                    WHEN 5 THEN N'CRAY_EX'
                    ELSE        N'AI_CLUSTER'
                END
        END                                                          AS sub_category,

        -- region
        CASE rn % 4
            WHEN 0 THEN N'NORTH_AMERICA'
            WHEN 1 THEN N'EMEA'
            WHEN 2 THEN N'APJ'
            ELSE        N'LATAM'
        END                                                          AS region,

        -- country
        CASE rn % 4
            WHEN 0 THEN CASE rn % 2 WHEN 0 THEN N'US' ELSE N'CA' END
            WHEN 1 THEN CASE rn % 3 WHEN 0 THEN N'DE' WHEN 1 THEN N'FR' ELSE N'UK' END
            WHEN 2 THEN CASE rn % 3 WHEN 0 THEN N'JP' WHEN 1 THEN N'IN' ELSE N'SG' END
            ELSE        CASE rn % 2 WHEN 0 THEN N'BR' ELSE N'MX' END
        END                                                          AS country,

        -- currency (~1.5% bad = XYZ every 67th row)
        CASE WHEN rn % 67 = 0 THEN N'XYZ'
             ELSE
                CASE rn % 4
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
    DATEADD(SECOND, -(rn % 86400), GETUTCDATE())  -- stagger modified_dt for watermark realism
FROM enriched
WHERE product_id IS NOT NULL   -- skip NULL PKs to avoid UNIQUE constraint error
   OR 1 = 1;                   -- include NULLs (they'll fail the UQ constraint, which is intentional for DQ)
GO

-- Verify
SELECT COUNT(*)        AS total_rows  FROM dbo.Forecast;
SELECT fiscal_period,  COUNT(*) AS row_count FROM dbo.Forecast GROUP BY fiscal_period ORDER BY fiscal_period;
SELECT category,       COUNT(*) AS row_count FROM dbo.Forecast GROUP BY category       ORDER BY row_count DESC;
SELECT MIN(modified_dt) AS oldest_watermark, MAX(modified_dt) AS latest_watermark FROM dbo.Forecast;
GO
