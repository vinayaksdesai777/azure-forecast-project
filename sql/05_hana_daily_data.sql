-- ============================================================
-- SAP HANA Cloud — Daily Forecast Data (Full Load Seed)
-- Schema: O9_SOURCE | Table: FORECAST_VIEW (base table)
-- Run in HANA Database Explorer (BTP Trial cockpit)
-- Target: 200,000 rows — 18 months daily x 500 products x 200 locations
-- (subset: 18 months x ~370 unique product-location pairs per day)
-- ============================================================

-- Create base table that FORECAST_VIEW sits on top of
CREATE COLUMN TABLE IF NOT EXISTS "O9_SOURCE"."FORECAST_DAILY" (
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
    "PERIOD_TYPE"     NVARCHAR(20)    DEFAULT 'DAILY',
    "FISCAL_PERIOD"   NVARCHAR(20),
    "CHANGED_ON"      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("PRODUCT_ID", "LOCATION_ID", "FORECAST_DATE")
);

-- Create view (ADF extracts from this)
CREATE OR REPLACE VIEW "O9_SOURCE"."FORECAST_VIEW" AS
    SELECT * FROM "O9_SOURCE"."FORECAST_DAILY";

-- ============================================================
-- Load 200,000 daily rows
-- 18 months: Jan-2024 to Jun-2025 (549 days)
-- ~365 product-location combos per day = 200,085 rows total
-- ~3% dirty categories, ~1.5% bad currencies, ~1% NULL product_id
-- ============================================================
CREATE OR REPLACE PROCEDURE "O9_SOURCE"."LOAD_DAILY_FORECAST"()
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
    DECLARE v_row           INTEGER := 0;
    DECLARE v_d             INTEGER;
    DECLARE v_i             INTEGER;
    DECLARE v_total_days    INTEGER := 549;   -- Jan 2024 to Jun 2025
    DECLARE v_per_day       INTEGER := 365;   -- product-location pairs per day

    DELETE FROM "O9_SOURCE"."FORECAST_DAILY";

    FOR v_d IN 1..v_total_days DO

        -- Advance date: start 2024-01-01, add (v_d - 1) days
        v_forecast_date := ADD_DAYS(TO_DATE('2024-01-01'), v_d - 1);

        -- Fiscal period: YYYY-MM
        v_fiscal := TO_NVARCHAR(YEAR(v_forecast_date)) || '-' ||
                    LPAD(TO_NVARCHAR(MONTH(v_forecast_date)), 2, '0');

        FOR v_i IN 1..v_per_day DO
            v_row := v_row + 1;

            -- Product: HPE-PROD-0001 to HPE-PROD-0500
            v_product_id  := 'HPE-PROD-' || LPAD(TO_NVARCHAR(MOD(v_row, 500) + 1), 4, '0');

            -- Location: LOC-001 to LOC-200
            v_location_id := 'LOC-' || LPAD(TO_NVARCHAR(MOD(v_row * 7, 200) + 1), 3, '0');

            -- Customer
            v_customer_id := 'CUST-' || LPAD(TO_NVARCHAR(MOD(v_row * 13, 50000) + 10000), 5, '0');

            -- Channel
            CASE MOD(v_row, 5)
                WHEN 0 THEN v_channel := 'DIRECT';
                WHEN 1 THEN v_channel := 'ONLINE';
                WHEN 2 THEN v_channel := 'PARTNER';
                WHEN 3 THEN v_channel := 'DISTRIBUTOR';
                ELSE        v_channel := 'VAR';
            END CASE;

            -- Base category (7 valid)
            CASE MOD(v_row, 7)
                WHEN 0 THEN v_category := 'SERVER';         v_sub_category := 'PROLIANT';
                WHEN 1 THEN v_category := 'STORAGE';        v_sub_category := 'PRIMERA';
                WHEN 2 THEN v_category := 'COMPUTE';        v_sub_category := 'SYNERGY';
                WHEN 3 THEN v_category := 'NETWORKING';     v_sub_category := 'ARUBA';
                WHEN 4 THEN v_category := 'PRIVATE_CLOUD';  v_sub_category := 'GREENLAKE';
                WHEN 5 THEN v_category := 'SUPERCOMPUTING'; v_sub_category := 'CRAY_EX';
                ELSE        v_category := 'AI';             v_sub_category := 'AI_CLUSTER';
            END CASE;

            -- Inject ~3% dirty categories
            CASE MOD(v_row, 100)
                WHEN 0 THEN v_category := 'HARDWARE'; v_sub_category := 'LEGACY';
                WHEN 1 THEN v_category := 'UNKNOWN';  v_sub_category := 'NA';
                WHEN 2 THEN v_category := 'MISC';     v_sub_category := 'OTHER';
                ELSE        v_category := v_category; v_sub_category := v_sub_category;
            END CASE;

            -- Region + currency
            CASE MOD(v_row, 4)
                WHEN 0 THEN v_region := 'NORTH_AMERICA'; v_currency := 'USD';
                WHEN 1 THEN v_region := 'EMEA';          v_currency := 'EUR';
                WHEN 2 THEN v_region := 'APJ';           v_currency := 'SGD';
                ELSE        v_region := 'LATAM';         v_currency := 'BRL';
            END CASE;

            -- Country by region
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

            -- ~1.5% bad currencies
            v_currency := CASE WHEN MOD(v_row, 67) = 0 THEN 'XYZ' ELSE v_currency END;

            -- Daily quantities (smaller than quarterly)
            v_qty     := TO_DECIMAL(MOD(v_row * 11, 4900) + 10, 12, 2);
            v_revenue := TO_DECIMAL(v_qty * (MOD(v_row * 31, 450) + 50), 16, 2);

            -- ~1% NULL product_id for quarantine testing
            v_product_id := CASE WHEN MOD(v_row, 100) = 99 THEN NULL ELSE v_product_id END;

            INSERT INTO "O9_SOURCE"."FORECAST_DAILY"
            VALUES (
                v_product_id, v_location_id, v_forecast_date,
                v_qty, v_revenue, v_customer_id, v_channel,
                v_category, v_sub_category, v_region, v_country,
                v_currency, 'UNIT', 'DAILY', v_fiscal,
                CURRENT_TIMESTAMP
            );
        END FOR;
    END FOR;

    COMMIT;
END;

-- Execute
CALL "O9_SOURCE"."LOAD_DAILY_FORECAST"();

-- Verify
SELECT COUNT(*) AS TOTAL_ROWS FROM "O9_SOURCE"."FORECAST_DAILY";

SELECT "FISCAL_PERIOD", COUNT(*) AS ROW_COUNT
FROM "O9_SOURCE"."FORECAST_DAILY"
GROUP BY "FISCAL_PERIOD"
ORDER BY "FISCAL_PERIOD";

SELECT "CATEGORY", COUNT(*) AS ROW_COUNT
FROM "O9_SOURCE"."FORECAST_DAILY"
GROUP BY "CATEGORY"
ORDER BY ROW_COUNT DESC;
