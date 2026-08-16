-- ============================================================
-- SQL Server — Weekly Forecast INCREMENTAL DELTA
-- Schema: dbo | Table: Forecast
-- Run AFTER 06_sqlserver_weekly_data.sql (full load Jan-Dec 2025)
-- Simulates ADF weekly runs: Jan 2026 to Jul 2026
-- Each run = ~8,500 new rows with modified_dt > last watermark
-- ADF watermark column: modified_dt
-- ============================================================

USE HPE_SOURCE;
GO

-- ============================================================
-- 26 weekly delta runs (Jan 2026 - Jul 2026)
-- Each run inserts ~8,500 rows (one week of product-location pairs)
-- modified_dt is set to the Monday of that week + 1 hour (post-extract)
-- ADF query: SELECT * FROM dbo.Forecast WHERE modified_dt > '<last_watermark>'
-- ============================================================
WITH
weeks AS (
    SELECT TOP 26
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS wk_offset
    FROM sys.all_columns
),
series AS (
    SELECT TOP 8500
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i
    FROM sys.all_columns
),
raw AS (
    SELECT
        w.wk_offset,
        s.i,
        -- Global row number, used only for attribute variation and the
        -- dirty-data cadence — never for the business key.
        w.wk_offset * 8500 + s.i + 1                                            AS rn,
        -- Business-key indexes derived from position WITHIN the week, so each
        -- week emits 8,500 distinct (product, location) pairs. Keying these to
        -- rn (product on rn % 500, location on (rn * 7) % 200) repeated the
        -- pair every lcm(500,200) = 1000 rows, so each 8,500-row week carried
        -- ~7,500 duplicates and the very first run failed the unique index.
        -- 43 product blocks x 200 locations = 8,600 possible, 8,500 used.
        -- prod_key rotates the per-week blocks through the 500-product
        -- catalogue as weeks advance, so products recur across runs. Computed
        -- here in `raw` (not in the enriched SELECT below) because SQL Server
        -- cannot reference a column alias from a sibling expression in the
        -- same SELECT list.
        (w.wk_offset * 43 + s.i / 200) % 500                                    AS prod_key,
        s.i % 200                                                               AS loc_ix,
        -- Mondays Jan 2026 onwards
        DATEADD(WEEK, w.wk_offset, CAST('2026-01-05' AS DATE))                  AS forecast_date,
        -- modified_dt = Monday of that week + 01:00 (watermark advances each week)
        DATEADD(HOUR, 1, CAST(DATEADD(WEEK, w.wk_offset,
            CAST('2026-01-05' AS DATE)) AS DATETIME2))                           AS modified_dt
    FROM weeks w CROSS JOIN series s
),
enriched AS (
    SELECT
        rn,
        prod_key,
        loc_ix,
        forecast_date,
        modified_dt,
        CAST(YEAR(forecast_date) AS NVARCHAR(4)) + N'-W'
            + RIGHT('0' + CAST(DATEPART(ISO_WEEK, forecast_date) AS NVARCHAR(2)), 2)
                                                                                 AS fiscal_period,

        -- ~0.5% NULL product_id (tighter than full load — data is cleaner in
        -- incremental). Excluded from the filtered unique index, so repeated
        -- NULLs are fine.
        CASE WHEN rn % 200 = 0 THEN NULL
             ELSE N'HPE-PROD-' + RIGHT('000' + CAST(prod_key + 1 AS NVARCHAR(4)), 4)
        END                                                                      AS product_id,

        N'LOC-' + RIGHT('00' + CAST(loc_ix + 1 AS NVARCHAR(3)), 3)             AS location_id,
        N'CUST-' + RIGHT('0000' + CAST((rn * 13) % 50000 + 10000 AS NVARCHAR(5)), 5)
                                                                                 AS customer_id,

        CASE rn % 5
            WHEN 0 THEN N'DIRECT' WHEN 1 THEN N'ONLINE' WHEN 2 THEN N'PARTNER'
            WHEN 3 THEN N'DISTRIBUTOR' ELSE N'VAR'
        END                                                                      AS channel,

        -- Attributes keyed to the same expressions that build product_id and
        -- location_id, matching the full-load script, so a product keeps its
        -- category and a location keeps its region/country across full and
        -- delta runs. Keying to rn alone made them properties of the row.
        CASE
            WHEN rn % 100 = 0 THEN N'HARDWARE'
            WHEN rn % 100 = 1 THEN N'UNKNOWN'
            ELSE CASE prod_key % 7
                WHEN 0 THEN N'SERVER'      WHEN 1 THEN N'STORAGE'
                WHEN 2 THEN N'COMPUTE'     WHEN 3 THEN N'NETWORKING'
                WHEN 4 THEN N'PRIVATE_CLOUD' WHEN 5 THEN N'SUPERCOMPUTING'
                ELSE N'AI'
            END
        END                                                                      AS category,

        CASE
            WHEN rn % 100 = 0 THEN N'LEGACY'
            WHEN rn % 100 = 1 THEN N'NA'
            ELSE CASE prod_key % 7
                WHEN 0 THEN N'PROLIANT'    WHEN 1 THEN N'PRIMERA'
                WHEN 2 THEN N'SYNERGY'     WHEN 3 THEN N'ARUBA'
                WHEN 4 THEN N'GREENLAKE'   WHEN 5 THEN N'CRAY_EX'
                ELSE N'AI_CLUSTER'
            END
        END                                                                      AS sub_category,

        CASE loc_ix % 4
            WHEN 0 THEN N'NORTH_AMERICA' WHEN 1 THEN N'EMEA'
            WHEN 2 THEN N'APJ'           ELSE N'LATAM'
        END                                                                      AS region,

        CASE loc_ix % 4
            WHEN 0 THEN CASE loc_ix % 2 WHEN 0 THEN N'US' ELSE N'CA' END
            WHEN 1 THEN CASE loc_ix % 3 WHEN 0 THEN N'DE' WHEN 1 THEN N'FR' ELSE N'UK' END
            WHEN 2 THEN CASE loc_ix % 3 WHEN 0 THEN N'JP' WHEN 1 THEN N'IN' ELSE N'SG' END
            ELSE        CASE loc_ix % 2 WHEN 0 THEN N'BR' ELSE N'MX' END
        END                                                                      AS country,

        -- Currency follows the region, so it stays consistent with the
        -- location-keyed region above.
        CASE WHEN rn % 67 = 0 THEN N'XYZ'
             ELSE CASE loc_ix % 4
                WHEN 0 THEN N'USD' WHEN 1 THEN N'EUR'
                WHEN 2 THEN N'SGD' ELSE N'BRL'
             END
        END                                                                      AS currency,

        CAST(rn * 11 % 9800 + 100 AS DECIMAL(12,2))                             AS forecast_qty,
        CAST((rn * 11 % 9800 + 100) * (rn * 31 % 450 + 50) AS DECIMAL(16,2))   AS revenue_amount
    FROM raw
)
INSERT INTO dbo.Forecast
    (product_id, location_id, forecast_date, forecast_qty, revenue_amount,
     customer_id, channel, category, sub_category, region, country,
     currency, uom, period_type, fiscal_period, modified_dt)
SELECT
    product_id, location_id, forecast_date, forecast_qty, revenue_amount,
    customer_id, channel, category, sub_category, region, country,
    currency, N'UNIT', N'WEEKLY', fiscal_period, modified_dt
FROM enriched;
GO

-- Verify watermark progression (ADF uses MAX(modified_dt) per run)
SELECT
    fiscal_period,
    COUNT(*)            AS rows,
    MIN(modified_dt)    AS run_start,
    MAX(modified_dt)    AS run_end
FROM dbo.Forecast
WHERE forecast_date >= '2026-01-01'
GROUP BY fiscal_period
ORDER BY fiscal_period;
GO

-- Business key must stay unique across full load + delta (expect zero rows)
SELECT product_id, location_id, forecast_date, COUNT(*) AS dupes
FROM dbo.Forecast
WHERE product_id IS NOT NULL
GROUP BY product_id, location_id, forecast_date
HAVING COUNT(*) > 1;
GO
