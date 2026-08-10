-- ============================================================
-- SAP HANA Cloud — Daily Forecast INCREMENTAL DELTA
-- Schema: O9_SOURCE | Table: FORECAST_DAILY
-- Run AFTER 05_hana_daily_data.sql (full load Jan-Dec 2025)
-- Simulates ADF incremental runs: Jan 2026 to Jul 2026
-- Each month = one ADF run, ~9,000 new rows with CHANGED_ON > last watermark
-- ============================================================
-- Month offsets from 2026-01-01:
--   Run 1: Jan 2026  (days 1-31,   ~9,000 rows, CHANGED_ON = 2026-01-31 00:05:00)
--   Run 2: Feb 2026  (days 32-59,  ~9,000 rows, CHANGED_ON = 2026-02-28 00:05:00)
--   Run 3: Mar 2026  (days 60-90,  ~9,000 rows, CHANGED_ON = 2026-03-31 00:05:00)
--   Run 4: Apr 2026  (days 91-120, ~9,000 rows, CHANGED_ON = 2026-04-30 00:05:00)
--   Run 5: May 2026  (days 121-151,~9,000 rows, CHANGED_ON = 2026-05-31 00:05:00)
--   Run 6: Jun 2026  (days 152-181,~9,000 rows, CHANGED_ON = 2026-06-30 00:05:00)
--   Run 7: Jul 2026  (days 182-212,~9,000 rows, CHANGED_ON = 2026-07-31 00:05:00)
-- ADF watermark query: WHERE CHANGED_ON > '<last_watermark>'
-- ============================================================

CREATE OR REPLACE PROCEDURE "O9_SOURCE"."LOAD_DAILY_DELTA"(
    IN v_month_start DATE,
    IN v_month_end   DATE,
    IN v_changed_on  TIMESTAMP
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
    DECLARE v_row           INTEGER := 0;
    DECLARE v_d             INTEGER;
    DECLARE v_i             INTEGER;
    DECLARE v_total_days    INTEGER;
    DECLARE v_per_day       INTEGER := 300;   -- 300 pairs/day = ~9,000 rows/month

    v_total_days := DAYS_BETWEEN(v_month_start, v_month_end) + 1;

    FOR v_d IN 1..v_total_days DO
        v_forecast_date := ADD_DAYS(v_month_start, v_d - 1);
        v_fiscal := TO_NVARCHAR(YEAR(v_forecast_date)) || '-'
                    || LPAD(TO_NVARCHAR(MONTH(v_forecast_date)), 2, '0');

        FOR v_i IN 1..v_per_day DO
            v_row := v_row + 1;

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

            -- Keyed to MOD(v_row, 500), matching v_product_id and the full-load
            -- script, so a product keeps its category across full and delta runs.
            CASE MOD(MOD(v_row, 500), 7)
                WHEN 0 THEN v_category := 'SERVER';         v_sub_category := 'PROLIANT';
                WHEN 1 THEN v_category := 'STORAGE';        v_sub_category := 'PRIMERA';
                WHEN 2 THEN v_category := 'COMPUTE';        v_sub_category := 'SYNERGY';
                WHEN 3 THEN v_category := 'NETWORKING';     v_sub_category := 'ARUBA';
                WHEN 4 THEN v_category := 'PRIVATE_CLOUD';  v_sub_category := 'GREENLAKE';
                WHEN 5 THEN v_category := 'SUPERCOMPUTING'; v_sub_category := 'CRAY_EX';
                ELSE        v_category := 'AI';             v_sub_category := 'AI_CLUSTER';
            END CASE;

            -- ~2% dirty categories in delta (less than full load)
            CASE MOD(v_row, 100)
                WHEN 0 THEN v_category := 'HARDWARE'; v_sub_category := 'LEGACY';
                WHEN 1 THEN v_category := 'UNKNOWN';  v_sub_category := 'NA';
                ELSE        v_category := v_category; v_sub_category := v_sub_category;
            END CASE;

            -- Keyed to MOD(v_row * 7, 200), matching v_location_id, so a
            -- location keeps its region and country across full and delta runs.
            CASE MOD(MOD(v_row * 7, 200), 4)
                WHEN 0 THEN v_region := 'NORTH_AMERICA'; v_currency := 'USD';
                WHEN 1 THEN v_region := 'EMEA';          v_currency := 'EUR';
                WHEN 2 THEN v_region := 'APJ';           v_currency := 'SGD';
                ELSE        v_region := 'LATAM';         v_currency := 'BRL';
            END CASE;

            CASE v_region
                WHEN 'NORTH_AMERICA' THEN
                    CASE MOD(MOD(v_row * 7, 200), 2) WHEN 0 THEN v_country := 'US'; ELSE v_country := 'CA'; END CASE;
                WHEN 'EMEA' THEN
                    CASE MOD(MOD(v_row * 7, 200), 3) WHEN 0 THEN v_country := 'DE'; WHEN 1 THEN v_country := 'FR'; ELSE v_country := 'UK'; END CASE;
                WHEN 'APJ' THEN
                    CASE MOD(MOD(v_row * 7, 200), 3) WHEN 0 THEN v_country := 'JP'; WHEN 1 THEN v_country := 'IN'; ELSE v_country := 'SG'; END CASE;
                ELSE
                    CASE MOD(MOD(v_row * 7, 200), 2) WHEN 0 THEN v_country := 'BR'; ELSE v_country := 'MX'; END CASE;
            END CASE;

            v_currency   := CASE WHEN MOD(v_row, 67) = 0 THEN 'XYZ' ELSE v_currency END;
            v_qty        := TO_DECIMAL(MOD(v_row * 11, 4900) + 10, 12, 2);
            v_revenue    := TO_DECIMAL(v_qty * (MOD(v_row * 31, 450) + 50), 16, 2);
            v_product_id := CASE WHEN MOD(v_row, 200) = 0 THEN NULL ELSE v_product_id END;

            -- UPSERT: new rows OR re-forecast updates for existing product-location-date
            UPSERT "O9_SOURCE"."FORECAST_DAILY" VALUES (
                v_product_id, v_location_id, v_forecast_date,
                v_qty, v_revenue, v_customer_id, v_channel,
                v_category, v_sub_category, v_region, v_country,
                v_currency, 'UNIT', 'DAILY', v_fiscal,
                v_changed_on     -- all rows in this batch share the same CHANGED_ON
            ) WHERE "PRODUCT_ID" = v_product_id
              AND   "LOCATION_ID" = v_location_id
              AND   "FORECAST_DATE" = v_forecast_date;
        END FOR;
    END FOR;

    COMMIT;
END;

-- ============================================================
-- Execute: one call per incremental ADF run
-- Run these one at a time, simulating each monthly ADF trigger.
-- Each call = one pipeline run with CHANGED_ON > previous run's timestamp.
-- ============================================================

-- Run 1 — Jan 2026 (~9,300 rows, CHANGED_ON = 2026-01-31 00:05:00)
CALL "O9_SOURCE"."LOAD_DAILY_DELTA"('2026-01-01', '2026-01-31', '2026-01-31 00:05:00');

-- Run 2 — Feb 2026 (~8,400 rows)
CALL "O9_SOURCE"."LOAD_DAILY_DELTA"('2026-02-01', '2026-02-28', '2026-02-28 00:05:00');

-- Run 3 — Mar 2026 (~9,300 rows)
CALL "O9_SOURCE"."LOAD_DAILY_DELTA"('2026-03-01', '2026-03-31', '2026-03-31 00:05:00');

-- Run 4 — Apr 2026 (~9,000 rows)
CALL "O9_SOURCE"."LOAD_DAILY_DELTA"('2026-04-01', '2026-04-30', '2026-04-30 00:05:00');

-- Run 5 — May 2026 (~9,300 rows)
CALL "O9_SOURCE"."LOAD_DAILY_DELTA"('2026-05-01', '2026-05-31', '2026-05-31 00:05:00');

-- Run 6 — Jun 2026 (~9,000 rows)
CALL "O9_SOURCE"."LOAD_DAILY_DELTA"('2026-06-01', '2026-06-30', '2026-06-30 00:05:00');

-- Run 7 — Jul 2026 (~9,300 rows)
CALL "O9_SOURCE"."LOAD_DAILY_DELTA"('2026-07-01', '2026-07-31', '2026-07-31 00:05:00');

-- Verify watermark progression
SELECT
    TO_NVARCHAR(YEAR("FORECAST_DATE")) || '-' || LPAD(TO_NVARCHAR(MONTH("FORECAST_DATE")), 2, '0') AS month,
    COUNT(*)            AS rows,
    MAX("CHANGED_ON")   AS latest_changed_on
FROM "O9_SOURCE"."FORECAST_DAILY"
WHERE "FORECAST_DATE" >= '2026-01-01'
GROUP BY TO_NVARCHAR(YEAR("FORECAST_DATE")) || '-' || LPAD(TO_NVARCHAR(MONTH("FORECAST_DATE")), 2, '0')
ORDER BY month;
