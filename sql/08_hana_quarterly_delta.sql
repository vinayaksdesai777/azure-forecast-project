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
    DECLARE v_prod_key      INTEGER;
    DECLARE v_loc_ix        INTEGER;

    FOR v_i IN 1..v_rows DO

        -- Business keys derived from position within the run, so all 10,000
        -- rows carry distinct (product, location) pairs. Keying product to
        -- MOD(v_i,500) and location to MOD(v_i*7,200) repeated the pair every
        -- LCM(500,200) = 1,000 rows, collapsing the run onto 1,000 real
        -- combinations. 50 blocks x 200 locations = exactly 10,000.
        v_prod_key := MOD((v_i - 1) / 200, 500);
        v_loc_ix   := MOD(v_i - 1, 200);

        v_product_id  := 'HPE-PROD-' || LPAD(TO_NVARCHAR(v_prod_key + 1), 4, '0');
        v_location_id := 'LOC-' || LPAD(TO_NVARCHAR(v_loc_ix + 1), 3, '0');
        v_customer_id := 'CUST-' || LPAD(TO_NVARCHAR(MOD(v_i * 13, 50000) + 10000), 5, '0');

        -- NOTE: CASE *expressions* assigned to variables, matching
        -- 05_hana_daily_data.sql. SAP HANA Cloud has removed the CASE
        -- *statement* from SQLScript, which raised "incorrect syntax near CASE".
        v_channel := CASE MOD(v_i, 5)
            WHEN 0 THEN 'DIRECT'
            WHEN 1 THEN 'ONLINE'
            WHEN 2 THEN 'PARTNER'
            WHEN 3 THEN 'DISTRIBUTOR'
            ELSE        'VAR'
        END;

        -- Keyed to v_prod_key, matching v_product_id and the full-load
        -- script, so a product keeps its category across full and delta runs.
        v_category := CASE MOD(v_prod_key, 7)
            WHEN 0 THEN 'SERVER'
            WHEN 1 THEN 'STORAGE'
            WHEN 2 THEN 'COMPUTE'
            WHEN 3 THEN 'NETWORKING'
            WHEN 4 THEN 'PRIVATE_CLOUD'
            WHEN 5 THEN 'SUPERCOMPUTING'
            ELSE        'AI'
        END;

        v_sub_category := CASE MOD(v_prod_key, 7)
            WHEN 0 THEN 'PROLIANT'
            WHEN 1 THEN 'PRIMERA'
            WHEN 2 THEN 'SYNERGY'
            WHEN 3 THEN 'ARUBA'
            WHEN 4 THEN 'GREENLAKE'
            WHEN 5 THEN 'CRAY_EX'
            ELSE        'AI_CLUSTER'
        END;

        -- Dirty-data overrides. The delta seeds only HARDWARE and UNKNOWN
        -- (no MISC) — narrower than the full load, matching the original.
        v_category := CASE MOD(v_i, 100)
            WHEN 0 THEN 'HARDWARE'
            WHEN 1 THEN 'UNKNOWN'
            ELSE        v_category
        END;

        v_sub_category := CASE MOD(v_i, 100)
            WHEN 0 THEN 'LEGACY'
            WHEN 1 THEN 'NA'
            ELSE        v_sub_category
        END;

        -- Keyed to v_loc_ix, matching v_location_id, so a location
        -- keeps its region and country across full and delta runs.
        v_region := CASE MOD(v_loc_ix, 4)
            WHEN 0 THEN 'NORTH_AMERICA'
            WHEN 1 THEN 'EMEA'
            WHEN 2 THEN 'APJ'
            ELSE        'LATAM'
        END;

        v_currency := CASE MOD(v_loc_ix, 4)
            WHEN 0 THEN 'USD'
            WHEN 1 THEN 'EUR'
            WHEN 2 THEN 'SGD'
            ELSE        'BRL'
        END;

        v_country := CASE
            WHEN MOD(v_loc_ix, 4) = 0 AND MOD(v_loc_ix, 2) = 0 THEN 'US'
            WHEN MOD(v_loc_ix, 4) = 0                          THEN 'CA'
            WHEN MOD(v_loc_ix, 4) = 1 AND MOD(v_loc_ix, 3) = 0 THEN 'DE'
            WHEN MOD(v_loc_ix, 4) = 1 AND MOD(v_loc_ix, 3) = 1 THEN 'FR'
            WHEN MOD(v_loc_ix, 4) = 1                          THEN 'UK'
            WHEN MOD(v_loc_ix, 4) = 2 AND MOD(v_loc_ix, 3) = 0 THEN 'JP'
            WHEN MOD(v_loc_ix, 4) = 2 AND MOD(v_loc_ix, 3) = 1 THEN 'IN'
            WHEN MOD(v_loc_ix, 4) = 2                          THEN 'SG'
            WHEN MOD(v_loc_ix, 2) = 0                          THEN 'BR'
            ELSE                                                    'MX'
        END;

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
