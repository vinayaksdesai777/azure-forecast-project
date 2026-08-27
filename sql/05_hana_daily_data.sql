-- ============================================================
-- SAP HANA Cloud — Daily Forecast Data (Full Load Seed)
-- Schema: O9_SOURCE | Tables: FORECAST_DAILY + FORECAST_VIEW
-- Target: ~547,800 rows — Jul 2020 to Jun 2025 (1,826 days x 300 rows/day)
-- Same instance, same schema (O9_SOURCE) as FORECAST_QUARTERLY
--
-- BTP Trial timeout workaround: procedure accepts day-range parameters.
-- Call in 18 batches of ~100 days each (30,000 rows per call, ~2-3 min each).
-- After full load: run 07_hana_daily_delta.sql for Jul 2025-Jun 2026 (~9k rows/run)
-- ============================================================

DO
BEGIN
    DECLARE tbl_count INT;

    SELECT COUNT(*) INTO tbl_count
    FROM SYS.TABLES
    WHERE SCHEMA_NAME = 'O9_SOURCE'
      AND TABLE_NAME  = 'FORECAST_DAILY';

    IF :tbl_count = 0 THEN
        EXEC '
        CREATE COLUMN TABLE "O9_SOURCE"."FORECAST_DAILY" (
            "PRODUCT_ID"      NVARCHAR(50)    NOT NULL,
            "LOCATION_ID"     NVARCHAR(50)    NOT NULL,
            "FORECAST_DATE"   DATE            NOT NULL,
            "FORECAST_QTY"    DECIMAL(12,2),
            "REVENUE_AMOUNT"  DECIMAL(16,2),
            "CUSTOMER_ID"     NVARCHAR(50),
            "CHANNEL"         NVARCHAR(50),
            "CATEGORY"        NVARCHAR(50),
            "SUB_CATEGORY"    NVARCHAR(50),
            "REGION"          NVARCHAR(50),
            "COUNTRY"         NVARCHAR(10),
            "CURRENCY"        NVARCHAR(10),
            "UOM"             NVARCHAR(20),
            "PERIOD_TYPE"     NVARCHAR(20)    DEFAULT ''DAILY'',
            "FISCAL_PERIOD"   NVARCHAR(20),
            "CHANGED_ON"      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
        )';
    END IF;
END;

-- Step 2: Create view (ADF reads from this; filtered by CHANGED_ON for incremental)
CREATE OR REPLACE VIEW "O9_SOURCE"."FORECAST_VIEW" AS
    SELECT * FROM "O9_SOURCE"."FORECAST_DAILY";

-- Step 3: Clear any previous data (safe to re-run)
DELETE FROM "O9_SOURCE"."FORECAST_DAILY";
COMMIT;

-- ============================================================
-- Step 4: Batch-insert procedure
-- v_day_start and v_day_end are 1-based day offsets from 2020-07-01.
--   Day 1    = 2020-07-01
--   Day 1826 = 2025-06-30
-- Each call inserts (v_day_end - v_day_start + 1) x 300 rows.
-- 100 days x 300 = 30,000 rows per call (~2 min on BTP trial).
-- Dirty-data pattern (same as quarterly):
--   ~3% invalid category (HARDWARE, UNKNOWN, MISC every 100th row)
--   ~1.5% bad currency (XYZ every 67th row)
--   ~1% NULL product_id (every 100th row within a day)
-- ============================================================
CREATE OR REPLACE PROCEDURE "O9_SOURCE"."LOAD_DAILY_FORECAST"(
    IN v_day_start INTEGER,
    IN v_day_end   INTEGER
)
LANGUAGE SQLSCRIPT AS
BEGIN
    DECLARE v_product_id    NVARCHAR(50);
    DECLARE v_location_id   NVARCHAR(50);
    DECLARE v_forecast_date DATE;
    DECLARE v_qty           DECIMAL(12,2);
    DECLARE v_revenue       DECIMAL(16,2);
    DECLARE v_customer_id   NVARCHAR(50);
    DECLARE v_channel       NVARCHAR(50);
    DECLARE v_category      NVARCHAR(50);
    DECLARE v_sub_category  NVARCHAR(50);
    DECLARE v_region        NVARCHAR(50);
    DECLARE v_country       NVARCHAR(10);
    DECLARE v_currency      NVARCHAR(10);
    DECLARE v_fiscal        NVARCHAR(20);
    DECLARE v_d             INTEGER;
    DECLARE v_i             INTEGER;
    DECLARE v_row           INTEGER;
    DECLARE v_per_day       INTEGER := 300;
    DECLARE v_base_date     DATE    := TO_DATE('2020-07-01');

    FOR v_d IN v_day_start..v_day_end DO

        v_forecast_date := ADD_DAYS(v_base_date, v_d - 1);

        v_fiscal := TO_NVARCHAR(YEAR(v_forecast_date)) || '-' ||
                    LPAD(TO_NVARCHAR(MONTH(v_forecast_date)), 2, '0');

        FOR v_i IN 1..v_per_day DO
            -- Global row counter preserves dirty-data spread across all batches
            v_row := (v_d - 1) * v_per_day + v_i;

            v_product_id  := 'HPE-PROD-' || LPAD(TO_NVARCHAR(MOD(v_row, 500) + 1), 4, '0');
            v_location_id := 'LOC-' || LPAD(TO_NVARCHAR(MOD(v_row * 7, 200) + 1), 3, '0');
            v_customer_id := 'CUST-' || LPAD(TO_NVARCHAR(MOD(v_row * 13, 50000) + 10000), 5, '0');

            v_channel := CASE MOD(v_row, 5)
                WHEN 0 THEN 'DIRECT'
                WHEN 1 THEN 'ONLINE'
                WHEN 2 THEN 'PARTNER'
                WHEN 3 THEN 'DISTRIBUTOR'
                ELSE        'VAR'
            END;

            -- Category is keyed to MOD(v_row, 500) — the same expression that
            -- builds v_product_id — so a product always carries the same
            -- category. MOD(v_row, 7) made category a property of the row: 500
            -- and 7 are coprime, so every product cycled through all 7
            -- categories and the Gold dimension churned a new SCD2 version on
            -- every load.
            v_category := CASE MOD(MOD(v_row, 500), 7)
                WHEN 0 THEN 'SERVER'
                WHEN 1 THEN 'STORAGE'
                WHEN 2 THEN 'COMPUTE'
                WHEN 3 THEN 'NETWORKING'
                WHEN 4 THEN 'PRIVATE_CLOUD'
                WHEN 5 THEN 'SUPERCOMPUTING'
                ELSE        'AI'
            END;

            v_sub_category := CASE MOD(MOD(v_row, 500), 7)
                WHEN 0 THEN 'PROLIANT'
                WHEN 1 THEN 'PRIMERA'
                WHEN 2 THEN 'SYNERGY'
                WHEN 3 THEN 'ARUBA'
                WHEN 4 THEN 'GREENLAKE'
                WHEN 5 THEN 'CRAY_EX'
                ELSE        'AI_CLUSTER'
            END;

            -- Dirty-data overrides
            v_category := CASE MOD(v_row, 100)
                WHEN 0 THEN 'HARDWARE'
                WHEN 1 THEN 'UNKNOWN'
                WHEN 2 THEN 'MISC'
                ELSE        v_category
            END;

            v_sub_category := CASE MOD(v_row, 100)
                WHEN 0 THEN 'LEGACY'
                WHEN 1 THEN 'NA'
                WHEN 2 THEN 'OTHER'
                ELSE        v_sub_category
            END;

            -- Region and country are keyed to MOD(v_row * 7, 200) — the same
            -- expression as v_location_id — so a location never moves between
            -- regions or countries across loads.
            v_region := CASE MOD(MOD(v_row * 7, 200), 4)
                WHEN 0 THEN 'NORTH_AMERICA'
                WHEN 1 THEN 'EMEA'
                WHEN 2 THEN 'APJ'
                ELSE        'LATAM'
            END;

            v_currency := CASE MOD(v_row, 4)
                WHEN 0 THEN 'USD'
                WHEN 1 THEN 'EUR'
                WHEN 2 THEN 'SGD'
                ELSE        'BRL'
            END;

            v_country := CASE
                WHEN v_region = 'NORTH_AMERICA' AND MOD(MOD(v_row * 7, 200), 2) = 0 THEN 'US'
                WHEN v_region = 'NORTH_AMERICA'                                     THEN 'CA'
                WHEN v_region = 'EMEA' AND MOD(MOD(v_row * 7, 200), 3) = 0          THEN 'DE'
                WHEN v_region = 'EMEA' AND MOD(MOD(v_row * 7, 200), 3) = 1          THEN 'FR'
                WHEN v_region = 'EMEA'                                              THEN 'UK'
                WHEN v_region = 'APJ' AND MOD(MOD(v_row * 7, 200), 3) = 0           THEN 'JP'
                WHEN v_region = 'APJ' AND MOD(MOD(v_row * 7, 200), 3) = 1           THEN 'IN'
                WHEN v_region = 'APJ'                                               THEN 'SG'
                WHEN MOD(MOD(v_row * 7, 200), 2) = 0                                THEN 'BR'
                ELSE                                                                     'MX'
            END;

            v_currency := CASE WHEN MOD(v_row, 67) = 0 THEN 'XYZ' ELSE v_currency END;

            v_qty     := TO_DECIMAL(MOD(v_row * 11, 4900) + 10, 12, 2);
            v_revenue := TO_DECIMAL(v_qty * (MOD(v_row * 31, 450) + 50), 16, 2);

            v_product_id := CASE WHEN MOD(v_i, 100) = 99 THEN NULL ELSE v_product_id END;

            INSERT INTO "O9_SOURCE"."FORECAST_DAILY"
            VALUES (
                v_product_id, v_location_id, v_forecast_date,
                v_qty, v_revenue, v_customer_id, v_channel,
                v_category, v_sub_category, v_region, v_country,
                v_currency, 'UNIT', 'DAILY', v_fiscal,
                -- CHANGED_ON must sit on the same timeline as FORECAST_DATE,
                -- not on the wall clock. CURRENT_TIMESTAMP put the full load in
                -- the seeding month while the delta scripts stamp their rows
                -- with the period they represent, so the delta looked OLDER
                -- than the full load and an incremental filtering
                -- CHANGED_ON > watermark returned nothing.
                ADD_SECONDS(TO_TIMESTAMP(v_forecast_date), MOD(v_row, 86400))
            );
        END FOR;
    END FOR;

    COMMIT;
END;

-- ============================================================
-- Step 5: Run in 19 batches (100 days each, last batch = 26 days)
-- Paste and run each CALL block separately in Database Explorer.
-- Wait for "Statement executed" before running the next one.
-- Total: 18 x 100 + 26 = 1,826 days → ~547,800 rows
-- ============================================================

ALTER TABLE "O9_SOURCE"."FORECAST_DAILY"
ALTER ("PRODUCT_ID" NVARCHAR(50) NULL);

-- Batch  1: days   1-100  (2020-07-01 to 2020-10-08)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(1, 100);

-- Batch  2: days 101-200  (2020-10-09 to 2021-01-16)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(101, 200);

-- Batch  3: days 201-300  (2021-01-17 to 2021-04-26)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(201, 300);

-- Batch  4: days 301-400  (2021-04-27 to 2021-08-04)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(301, 400);

-- Batch  5: days 401-500  (2021-08-05 to 2021-11-12)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(401, 500);

-- Batch  6: days 501-600  (2021-11-13 to 2022-02-21)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(501, 600);

-- Batch  7: days 601-700  (2022-02-22 to 2022-06-01)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(601, 700);

-- Batch  8: days 701-800  (2022-06-02 to 2022-09-09)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(701, 800);

-- Batch  9: days 801-900  (2022-09-10 to 2022-12-18)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(801, 900);

-- Batch 10: days  901-1000 (2022-12-19 to 2023-03-28)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(901, 1000);

-- Batch 11: days 1001-1100 (2023-03-29 to 2023-07-06)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(1001, 1100);

-- Batch 12: days 1101-1200 (2023-07-07 to 2023-10-14)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(1101, 1200);

-- Batch 13: days 1201-1300 (2023-10-15 to 2024-01-22)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(1201, 1300);

-- Batch 14: days 1301-1400 (2024-01-23 to 2024-05-01)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(1301, 1400);

-- Batch 15: days 1401-1500 (2024-05-02 to 2024-08-09)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(1401, 1500);

-- Batch 16: days 1501-1600 (2024-08-10 to 2024-11-17)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(1501, 1600);

-- Batch 17: days 1601-1700 (2024-11-18 to 2025-02-25)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(1601, 1700);

-- Batch 18: days 1701-1800 (2025-02-26 to 2025-06-05)
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(1701, 1800);

-- Batch 19: days 1801-1826 (2025-06-06 to 2025-06-30) ← final batch
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"(1801, 1826);

-- ============================================================
-- Step 6: Verify after all batches complete
-- ============================================================
SELECT COUNT(*) AS TOTAL_ROWS FROM "O9_SOURCE"."FORECAST_DAILY";
-- Expected: ~547,800

SELECT "FISCAL_PERIOD", COUNT(*) AS ROW_COUNT
FROM "O9_SOURCE"."FORECAST_DAILY"
GROUP BY "FISCAL_PERIOD"
ORDER BY "FISCAL_PERIOD";
-- Expected: 60 months, ~9,130 rows each

SELECT "CATEGORY", COUNT(*) AS ROW_COUNT
FROM "O9_SOURCE"."FORECAST_DAILY"
GROUP BY "CATEGORY"
ORDER BY ROW_COUNT DESC;
-- Expected: 7 valid + 3 dirty (HARDWARE, UNKNOWN, MISC)

SELECT MIN("FORECAST_DATE") AS FIRST_DAY, MAX("FORECAST_DATE") AS LAST_DAY
FROM "O9_SOURCE"."FORECAST_DAILY";
-- Expected: 2020-07-01 to 2025-06-30
