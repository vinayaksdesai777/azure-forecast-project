-- ============================================================
-- SAP HANA Cloud — Quarterly Forecast Data (Full Load Seed)
-- Schema: O9_SOURCE | Table: FORECAST_QUARTERLY
-- Target: 600,000 rows — 20 quarters (Q3-2020 to Q2-2025) x 30,000 rows each
-- Same instance, same schema (O9_SOURCE) as FORECAST_DAILY
--
-- BTP Trial timeout workaround: procedure accepts quarter-range parameters.
-- Call in 4 batches of 5 quarters (150,000 rows per call, ~2-3 min each).
-- After full load: run 08_hana_quarterly_delta.sql for Q3-2025 onwards (10k rows/run)
-- ============================================================

-- Step 1: Create table (idempotent)
CREATE COLUMN TABLE IF NOT EXISTS "O9_SOURCE"."FORECAST_QUARTERLY" (
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
    "PERIOD_TYPE"     NVARCHAR(20)    DEFAULT 'QUARTERLY',
    "FISCAL_PERIOD"   NVARCHAR(20),
    "CHANGED_ON"      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- Step 2: Clear any previous data (safe to re-run)
DELETE FROM "O9_SOURCE"."FORECAST_QUARTERLY";
COMMIT;

-- ============================================================
-- Step 3: Batch-insert procedure
-- v_q_start and v_q_end are 1-based quarter indices:
--   1  = Q3-2020 (2020-07-01)   11 = Q1-2023 (2023-01-01)
--   2  = Q4-2020 (2020-10-01)   12 = Q2-2023 (2023-04-01)
--   3  = Q1-2021 (2021-01-01)   13 = Q3-2023 (2023-07-01)
--   4  = Q2-2021 (2021-04-01)   14 = Q4-2023 (2023-10-01)
--   5  = Q3-2021 (2021-07-01)   15 = Q1-2024 (2024-01-01)
--   6  = Q4-2021 (2021-10-01)   16 = Q2-2024 (2024-04-01)
--   7  = Q1-2022 (2022-01-01)   17 = Q3-2024 (2024-07-01)
--   8  = Q2-2022 (2022-04-01)   18 = Q4-2024 (2024-10-01)
--   9  = Q3-2022 (2022-07-01)   19 = Q1-2025 (2025-01-01)
--  10  = Q4-2022 (2022-10-01)   20 = Q2-2025 (2025-04-01)
-- 30,000 rows per quarter. 5 quarters x 30,000 = 150,000 rows per call.
-- Dirty-data pattern:
--   ~3% invalid category (HARDWARE, UNKNOWN, MISC every 100th row)
--   ~1.5% bad currency (XYZ every 67th row)
--   ~1% NULL product_id (every 100th row within a quarter)
-- ============================================================
CREATE OR REPLACE PROCEDURE "O9_SOURCE"."LOAD_QUARTERLY_FORECAST"(
    IN v_q_start INTEGER,
    IN v_q_end   INTEGER
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
    DECLARE v_q             INTEGER;
    DECLARE v_i             INTEGER;
    DECLARE v_row           INTEGER;
    DECLARE v_rows_per_q    INTEGER := 30000;

    FOR v_q IN v_q_start..v_q_end DO

        -- Map quarter index to start date and fiscal label
        CASE v_q
            WHEN  1 THEN v_forecast_date := TO_DATE('2020-07-01'); v_fiscal := '2020-Q3';
            WHEN  2 THEN v_forecast_date := TO_DATE('2020-10-01'); v_fiscal := '2020-Q4';
            WHEN  3 THEN v_forecast_date := TO_DATE('2021-01-01'); v_fiscal := '2021-Q1';
            WHEN  4 THEN v_forecast_date := TO_DATE('2021-04-01'); v_fiscal := '2021-Q2';
            WHEN  5 THEN v_forecast_date := TO_DATE('2021-07-01'); v_fiscal := '2021-Q3';
            WHEN  6 THEN v_forecast_date := TO_DATE('2021-10-01'); v_fiscal := '2021-Q4';
            WHEN  7 THEN v_forecast_date := TO_DATE('2022-01-01'); v_fiscal := '2022-Q1';
            WHEN  8 THEN v_forecast_date := TO_DATE('2022-04-01'); v_fiscal := '2022-Q2';
            WHEN  9 THEN v_forecast_date := TO_DATE('2022-07-01'); v_fiscal := '2022-Q3';
            WHEN 10 THEN v_forecast_date := TO_DATE('2022-10-01'); v_fiscal := '2022-Q4';
            WHEN 11 THEN v_forecast_date := TO_DATE('2023-01-01'); v_fiscal := '2023-Q1';
            WHEN 12 THEN v_forecast_date := TO_DATE('2023-04-01'); v_fiscal := '2023-Q2';
            WHEN 13 THEN v_forecast_date := TO_DATE('2023-07-01'); v_fiscal := '2023-Q3';
            WHEN 14 THEN v_forecast_date := TO_DATE('2023-10-01'); v_fiscal := '2023-Q4';
            WHEN 15 THEN v_forecast_date := TO_DATE('2024-01-01'); v_fiscal := '2024-Q1';
            WHEN 16 THEN v_forecast_date := TO_DATE('2024-04-01'); v_fiscal := '2024-Q2';
            WHEN 17 THEN v_forecast_date := TO_DATE('2024-07-01'); v_fiscal := '2024-Q3';
            WHEN 18 THEN v_forecast_date := TO_DATE('2024-10-01'); v_fiscal := '2024-Q4';
            WHEN 19 THEN v_forecast_date := TO_DATE('2025-01-01'); v_fiscal := '2025-Q1';
            WHEN 20 THEN v_forecast_date := TO_DATE('2025-04-01'); v_fiscal := '2025-Q2';
        END CASE;

        FOR v_i IN 1..v_rows_per_q DO
            -- Global row counter preserves dirty-data spread across all batches
            v_row := (v_q - 1) * v_rows_per_q + v_i;

            v_product_id  := 'HPE-PROD-' || LPAD(TO_NVARCHAR(MOD(v_row, 500) + 1), 4, '0');
            v_location_id := 'LOC-' || LPAD(TO_NVARCHAR(MOD(v_row * 7, 200) + 1), 3, '0');
            v_customer_id := 'CUST-' || LPAD(TO_NVARCHAR(MOD(v_row * 13, 50000) + 10000), 5, '0');

            CASE MOD(v_row, 5)
                WHEN 0 THEN v_channel := 'DIRECT';
                WHEN 1 THEN v_channel := 'ONLINE';
                WHEN 2 THEN v_channel := 'PARTNER';
                WHEN 3 THEN v_channel := 'DISTRIBUTOR';
                ELSE        v_channel := 'VAR';
            END CASE;

            CASE MOD(v_row, 7)
                WHEN 0 THEN v_category := 'SERVER';         v_sub_category := 'PROLIANT';
                WHEN 1 THEN v_category := 'STORAGE';        v_sub_category := 'PRIMERA';
                WHEN 2 THEN v_category := 'COMPUTE';        v_sub_category := 'SYNERGY';
                WHEN 3 THEN v_category := 'NETWORKING';     v_sub_category := 'ARUBA';
                WHEN 4 THEN v_category := 'PRIVATE_CLOUD';  v_sub_category := 'GREENLAKE';
                WHEN 5 THEN v_category := 'SUPERCOMPUTING'; v_sub_category := 'CRAY_EX';
                ELSE        v_category := 'AI';             v_sub_category := 'AI_CLUSTER';
            END CASE;

            CASE MOD(v_row, 100)
                WHEN 0 THEN v_category := 'HARDWARE'; v_sub_category := 'LEGACY';
                WHEN 1 THEN v_category := 'UNKNOWN';  v_sub_category := 'NA';
                WHEN 2 THEN v_category := 'MISC';     v_sub_category := 'OTHER';
                ELSE        v_category := v_category; v_sub_category := v_sub_category;
            END CASE;

            CASE MOD(v_row, 4)
                WHEN 0 THEN v_region := 'NORTH_AMERICA'; v_currency := 'USD';
                WHEN 1 THEN v_region := 'EMEA';          v_currency := 'EUR';
                WHEN 2 THEN v_region := 'APJ';           v_currency := 'SGD';
                ELSE        v_region := 'LATAM';         v_currency := 'BRL';
            END CASE;

            CASE v_region
                WHEN 'NORTH_AMERICA' THEN
                    CASE MOD(v_row, 2) WHEN 0 THEN v_country := 'US'; ELSE v_country := 'CA'; END CASE;
                WHEN 'EMEA' THEN
                    CASE MOD(v_row, 3) WHEN 0 THEN v_country := 'DE'; WHEN 1 THEN v_country := 'FR'; ELSE v_country := 'UK'; END CASE;
                WHEN 'APJ' THEN
                    CASE MOD(v_row, 3) WHEN 0 THEN v_country := 'JP'; WHEN 1 THEN v_country := 'IN'; ELSE v_country := 'SG'; END CASE;
                ELSE
                    CASE MOD(v_row, 2) WHEN 0 THEN v_country := 'BR'; ELSE v_country := 'MX'; END CASE;
            END CASE;

            v_currency := CASE WHEN MOD(v_row, 67) = 0 THEN 'XYZ' ELSE v_currency END;

            -- Quarterly quantities (larger buckets than daily)
            v_qty     := TO_DECIMAL(MOD(v_row * 17, 49000) + 1000, 12, 2);
            v_revenue := TO_DECIMAL(v_qty * (MOD(v_row * 31, 450) + 50), 16, 2);

            v_product_id := CASE WHEN MOD(v_i, 100) = 99 THEN NULL ELSE v_product_id END;

            INSERT INTO "O9_SOURCE"."FORECAST_QUARTERLY"
            VALUES (
                v_product_id, v_location_id, v_forecast_date,
                v_qty, v_revenue, v_customer_id, v_channel,
                v_category, v_sub_category, v_region, v_country,
                v_currency, 'UNIT', 'QUARTERLY', v_fiscal,
                CURRENT_TIMESTAMP
            );
        END FOR;
    END FOR;

    COMMIT;
END;

-- ============================================================
-- Step 4: Run in 4 batches (5 quarters each = 150,000 rows per call)
-- Paste and run each CALL block separately in Database Explorer.
-- Wait for "Statement executed" before running the next one.
-- Total: 4 x 5 quarters x 30,000 = 600,000 rows
-- ============================================================

-- Batch 1: Q3-2020 to Q3-2021  (quarters  1-5)
CALL "O9_SOURCE"."LOAD_QUARTERLY_FORECAST"(1, 5);

-- Batch 2: Q4-2021 to Q4-2022  (quarters  6-10)
CALL "O9_SOURCE"."LOAD_QUARTERLY_FORECAST"(6, 10);

-- Batch 3: Q1-2023 to Q1-2024  (quarters 11-15)
CALL "O9_SOURCE"."LOAD_QUARTERLY_FORECAST"(11, 15);

-- Batch 4: Q2-2024 to Q2-2025  (quarters 16-20) ← final batch
CALL "O9_SOURCE"."LOAD_QUARTERLY_FORECAST"(16, 20);

-- ============================================================
-- Step 5: Verify after all batches complete
-- ============================================================
SELECT COUNT(*) AS TOTAL_ROWS FROM "O9_SOURCE"."FORECAST_QUARTERLY";
-- Expected: 600,000

SELECT "FISCAL_PERIOD", COUNT(*) AS ROW_COUNT
FROM "O9_SOURCE"."FORECAST_QUARTERLY"
GROUP BY "FISCAL_PERIOD"
ORDER BY "FISCAL_PERIOD";
-- Expected: 20 quarters, 30,000 rows each

SELECT "CATEGORY", COUNT(*) AS ROW_COUNT
FROM "O9_SOURCE"."FORECAST_QUARTERLY"
GROUP BY "CATEGORY"
ORDER BY ROW_COUNT DESC;
-- Expected: 7 valid + 3 dirty (HARDWARE, UNKNOWN, MISC)

SELECT MIN("FORECAST_DATE") AS FIRST_QTR, MAX("FORECAST_DATE") AS LAST_QTR
FROM "O9_SOURCE"."FORECAST_QUARTERLY";
-- Expected: 2020-07-01 to 2025-04-01
