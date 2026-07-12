-- ============================================================
-- SAP HANA Cloud — Quarterly Forecast INCREMENTAL DELTA
-- Schema: O9_SOURCE | Table: FORECAST_QUARTERLY
-- Run AFTER 04_hana_quarterly_data.sql (full load Q1-Q4 2025)
-- Simulates ADF quarterly runs: Q1-2026 and Q2-2026
-- Each run = ~10,000 new/revised rows with CHANGED_ON > last watermark
-- ============================================================

CREATE OR REPLACE PROCEDURE "O9_SOURCE"."LOAD_QUARTERLY_DELTA"(
    IN v_qtr_start  DATE,
    IN v_fiscal_in  NVARCHAR(20),
    IN v_changed_on TIMESTAMP
)
LANGUAGE SQLSCRIPT AS
BEGIN
    DECLARE v_product_id    NVARCHAR(50);
    DECLARE v_location_id   NVARCHAR(50);
    DECLARE v_qty           DECIMAL(12,2);
    DECLARE v_revenue       DECIMAL(16,2);
    DECLARE v_customer_id   NVARCHAR(50);
    DECLARE v_channel       NVARCHAR(50);
    DECLARE v_category      NVARCHAR(50);
    DECLARE v_sub_category  NVARCHAR(50);
    DECLARE v_region        NVARCHAR(50);
    DECLARE v_country       NVARCHAR(10);
    DECLARE v_currency      NVARCHAR(10);
    DECLARE v_i             INTEGER;
    DECLARE v_rows          INTEGER := 10000;

    FOR v_i IN 1..v_rows DO

        v_product_id  := 'HPE-PROD-' || LPAD(TO_NVARCHAR(MOD(v_i, 500) + 1), 4, '0');
        v_location_id := 'LOC-' || LPAD(TO_NVARCHAR(MOD(v_i * 7, 200) + 1), 3, '0');
        v_customer_id := 'CUST-' || LPAD(TO_NVARCHAR(MOD(v_i * 13, 50000) + 10000), 5, '0');

        CASE MOD(v_i, 5)
            WHEN 0 THEN v_channel := 'DIRECT';
            WHEN 1 THEN v_channel := 'ONLINE';
            WHEN 2 THEN v_channel := 'PARTNER';
            WHEN 3 THEN v_channel := 'DISTRIBUTOR';
            ELSE        v_channel := 'VAR';
        END CASE;

        CASE MOD(v_i, 7)
            WHEN 0 THEN v_category := 'SERVER';         v_sub_category := 'PROLIANT';
            WHEN 1 THEN v_category := 'STORAGE';        v_sub_category := 'PRIMERA';
            WHEN 2 THEN v_category := 'COMPUTE';        v_sub_category := 'SYNERGY';
            WHEN 3 THEN v_category := 'NETWORKING';     v_sub_category := 'ARUBA';
            WHEN 4 THEN v_category := 'PRIVATE_CLOUD';  v_sub_category := 'GREENLAKE';
            WHEN 5 THEN v_category := 'SUPERCOMPUTING'; v_sub_category := 'CRAY_EX';
            ELSE        v_category := 'AI';             v_sub_category := 'AI_CLUSTER';
        END CASE;

        CASE MOD(v_i, 100)
            WHEN 0 THEN v_category := 'HARDWARE'; v_sub_category := 'LEGACY';
            WHEN 1 THEN v_category := 'UNKNOWN';  v_sub_category := 'NA';
            ELSE        v_category := v_category; v_sub_category := v_sub_category;
        END CASE;

        CASE MOD(v_i, 4)
            WHEN 0 THEN v_region := 'NORTH_AMERICA'; v_currency := 'USD';
            WHEN 1 THEN v_region := 'EMEA';          v_currency := 'EUR';
            WHEN 2 THEN v_region := 'APJ';           v_currency := 'SGD';
            ELSE        v_region := 'LATAM';         v_currency := 'BRL';
        END CASE;

        CASE v_region
            WHEN 'NORTH_AMERICA' THEN
                CASE MOD(v_i, 2) WHEN 0 THEN v_country := 'US'; ELSE v_country := 'CA'; END CASE;
            WHEN 'EMEA' THEN
                CASE MOD(v_i, 3) WHEN 0 THEN v_country := 'DE'; WHEN 1 THEN v_country := 'FR'; ELSE v_country := 'UK'; END CASE;
            WHEN 'APJ' THEN
                CASE MOD(v_i, 3) WHEN 0 THEN v_country := 'JP'; WHEN 1 THEN v_country := 'IN'; ELSE v_country := 'SG'; END CASE;
            ELSE
                CASE MOD(v_i, 2) WHEN 0 THEN v_country := 'BR'; ELSE v_country := 'MX'; END CASE;
        END CASE;

        v_currency   := CASE WHEN MOD(v_i, 67) = 0 THEN 'XYZ' ELSE v_currency END;
        v_qty        := TO_DECIMAL(MOD(v_i * 17, 49000) + 1000, 12, 2);
        v_revenue    := TO_DECIMAL(v_qty * (MOD(v_i * 31, 450) + 50), 16, 2);
        v_product_id := CASE WHEN MOD(v_i, 200) = 0 THEN NULL ELSE v_product_id END;

        UPSERT "O9_SOURCE"."FORECAST_QUARTERLY" VALUES (
            v_product_id, v_location_id, v_qtr_start,
            v_qty, v_revenue, v_customer_id, v_channel,
            v_category, v_sub_category, v_region, v_country,
            v_currency, 'UNIT', 'QUARTERLY', v_fiscal_in,
            v_changed_on
        ) WHERE "PRODUCT_ID"    = v_product_id
          AND   "LOCATION_ID"   = v_location_id
          AND   "FORECAST_DATE" = v_qtr_start;

    END FOR;
    COMMIT;
END;

-- Run 1 — Q1-2026: 10,000 new quarterly forecasts
CALL "O9_SOURCE"."LOAD_QUARTERLY_DELTA"('2026-01-01', '2026-Q1', '2026-01-15 00:05:00');

-- Run 2 — Q2-2026: 10,000 new quarterly forecasts
CALL "O9_SOURCE"."LOAD_QUARTERLY_DELTA"('2026-04-01', '2026-Q2', '2026-04-15 00:05:00');

-- Verify
SELECT "FISCAL_PERIOD", COUNT(*) AS rows, MAX("CHANGED_ON") AS latest_changed_on
FROM "O9_SOURCE"."FORECAST_QUARTERLY"
GROUP BY "FISCAL_PERIOD"
ORDER BY "FISCAL_PERIOD";
