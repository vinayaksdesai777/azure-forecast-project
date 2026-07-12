-- ============================================================
-- SAP HANA Cloud — Quarterly Forecast Data (Full Load Seed)
-- Schema: O9_SOURCE
-- Run in HANA Database Explorer (BTP Trial cockpit)
-- ============================================================

-- Create quarterly table (same structure as daily FORECAST_VIEW base table)
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
    "CHANGED_ON"      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("PRODUCT_ID", "LOCATION_ID", "FORECAST_DATE")
);

-- ============================================================
-- FULL LOAD: 100,000 quarterly rows — 4 quarters (Q1-2025 to Q4-2025)
-- 25,000 rows per quarter across 500 products x 200 locations
-- After this: run 08_hana_quarterly_delta.sql for Q1-Q2 2026 incremental (10k rows/run)
-- Includes intentional dirty data (~3%) for DQ pipeline testing
-- ============================================================

-- Use a procedure to generate bulk data efficiently in HANA
CREATE OR REPLACE PROCEDURE "O9_SOURCE"."LOAD_QUARTERLY_FORECAST"()
LANGUAGE SQLSCRIPT AS
BEGIN
    DECLARE v_product_id   NVARCHAR(50);
    DECLARE v_location_id  NVARCHAR(50);
    DECLARE v_forecast_date DATE;
    DECLARE v_qty          DECIMAL(12,2);
    DECLARE v_revenue      DECIMAL(16,2);
    DECLARE v_customer_id  NVARCHAR(50);
    DECLARE v_channel      NVARCHAR(50);
    DECLARE v_category     NVARCHAR(50);
    DECLARE v_sub_category NVARCHAR(50);
    DECLARE v_region       NVARCHAR(50);
    DECLARE v_country      NVARCHAR(10);
    DECLARE v_currency     NVARCHAR(10);
    DECLARE v_fiscal       NVARCHAR(20);
    DECLARE v_i            INTEGER;
    DECLARE v_row          INTEGER := 0;

    -- Lookup arrays (simulated via CASE)
    DECLARE v_quarters     INTEGER := 4;
    DECLARE v_rows_per_q   INTEGER := 25000;
    DECLARE v_q            INTEGER;

    DELETE FROM "O9_SOURCE"."FORECAST_QUARTERLY";

    FOR v_q IN 1..v_quarters DO
        -- Full load: Q1-2025 to Q4-2025 only
        CASE v_q
            WHEN 1 THEN v_forecast_date := '2025-01-01'; v_fiscal := '2025-Q1';
            WHEN 2 THEN v_forecast_date := '2025-04-01'; v_fiscal := '2025-Q2';
            WHEN 3 THEN v_forecast_date := '2025-07-01'; v_fiscal := '2025-Q3';
            WHEN 4 THEN v_forecast_date := '2025-10-01'; v_fiscal := '2025-Q4';
        END CASE;

        FOR v_i IN 1..v_rows_per_q DO
            v_row := v_row + 1;

            -- Product: HPE-PROD-0001 to HPE-PROD-0500
            v_product_id  := 'HPE-PROD-' || LPAD(TO_NVARCHAR(MOD(v_row, 500) + 1), 4, '0');

            -- Location: LOC-001 to LOC-200
            v_location_id := 'LOC-' || LPAD(TO_NVARCHAR(MOD(v_row * 7, 200) + 1), 3, '0');

            -- Customer
            v_customer_id := 'CUST-' || LPAD(TO_NVARCHAR(MOD(v_row * 13, 50000) + 10000), 5, '0');

            -- Channel rotation
            CASE MOD(v_row, 5)
                WHEN 0 THEN v_channel := 'DIRECT';
                WHEN 1 THEN v_channel := 'ONLINE';
                WHEN 2 THEN v_channel := 'PARTNER';
                WHEN 3 THEN v_channel := 'DISTRIBUTOR';
                ELSE        v_channel := 'VAR';
            END CASE;

            -- Base category rotation (7 valid HPE categories)
            CASE MOD(v_row, 7)
                WHEN 0 THEN v_category := 'SERVER';         v_sub_category := 'PROLIANT';
                WHEN 1 THEN v_category := 'STORAGE';        v_sub_category := 'PRIMERA';
                WHEN 2 THEN v_category := 'COMPUTE';        v_sub_category := 'SYNERGY';
                WHEN 3 THEN v_category := 'NETWORKING';     v_sub_category := 'ARUBA';
                WHEN 4 THEN v_category := 'PRIVATE_CLOUD';  v_sub_category := 'GREENLAKE';
                WHEN 5 THEN v_category := 'SUPERCOMPUTING'; v_sub_category := 'CRAY_EX';
                ELSE        v_category := 'AI';             v_sub_category := 'AI_CLUSTER';
            END CASE;

            -- Inject ~3% dirty categories (override every 100th row pattern)
            CASE MOD(v_row, 100)
                WHEN 0 THEN v_category := 'HARDWARE'; v_sub_category := 'LEGACY';
                WHEN 1 THEN v_category := 'UNKNOWN';  v_sub_category := 'NA';
                WHEN 2 THEN v_category := 'MISC';     v_sub_category := 'OTHER';
                ELSE        v_category := v_category; v_sub_category := v_sub_category;
            END CASE;

            -- Region + currency by quadrant
            CASE MOD(v_row, 4)
                WHEN 0 THEN v_region := 'NORTH_AMERICA'; v_currency := 'USD';
                WHEN 1 THEN v_region := 'EMEA';          v_currency := 'EUR';
                WHEN 2 THEN v_region := 'APJ';           v_currency := 'SGD';
                ELSE        v_region := 'LATAM';         v_currency := 'BRL';
            END CASE;

            -- Country by region
            CASE v_region
                WHEN 'NORTH_AMERICA' THEN
                    CASE MOD(v_row, 2)
                        WHEN 0 THEN v_country := 'US';
                        ELSE        v_country := 'CA';
                    END CASE;
                WHEN 'EMEA' THEN
                    CASE MOD(v_row, 3)
                        WHEN 0 THEN v_country := 'DE';
                        WHEN 1 THEN v_country := 'FR';
                        ELSE        v_country := 'UK';
                    END CASE;
                WHEN 'APJ' THEN
                    CASE MOD(v_row, 3)
                        WHEN 0 THEN v_country := 'JP';
                        WHEN 1 THEN v_country := 'IN';
                        ELSE        v_country := 'SG';
                    END CASE;
                ELSE
                    CASE MOD(v_row, 2)
                        WHEN 0 THEN v_country := 'BR';
                        ELSE        v_country := 'MX';
                    END CASE;
            END CASE;

            -- Inject ~1.5% bad currencies
            v_currency := CASE WHEN MOD(v_row, 67) = 0 THEN 'XYZ' ELSE v_currency END;

            -- Quantities and revenue (quarterly = larger buckets than daily)
            v_qty     := TO_DECIMAL(MOD(v_row * 17, 49000) + 1000, 12, 2);
            v_revenue := TO_DECIMAL(v_qty * (MOD(v_row * 31, 450) + 50), 16, 2);

            -- ~1% NULL product_id for quarantine testing
            v_product_id := CASE WHEN MOD(v_row, 100) = 99 THEN NULL ELSE v_product_id END;

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

-- Execute the procedure
CALL "O9_SOURCE"."LOAD_QUARTERLY_FORECAST"();

-- Verify
SELECT "FISCAL_PERIOD", COUNT(*) AS ROW_COUNT
FROM "O9_SOURCE"."FORECAST_QUARTERLY"
GROUP BY "FISCAL_PERIOD"
ORDER BY "FISCAL_PERIOD";

SELECT "CATEGORY", COUNT(*) AS ROW_COUNT
FROM "O9_SOURCE"."FORECAST_QUARTERLY"
GROUP BY "CATEGORY"
ORDER BY ROW_COUNT DESC;

SELECT COUNT(*) AS TOTAL FROM "O9_SOURCE"."FORECAST_QUARTERLY";
