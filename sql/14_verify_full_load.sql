-- ============================================================
-- Full-load verification.
--
-- Run top to bottom in a Databricks SQL editor after a full load of all four
-- subjects. Every query states what a PASS looks like. Nothing here writes.
--
-- The checks are ordered so a failure explains the ones below it: audit status
-- first (did it run?), then row reconciliation (did the rows survive?), then
-- the star schema (is the model right?), then DQ (what was rejected?).
-- ============================================================


-- ============================================================
-- 1. Did every layer of every subject succeed?
--
-- PASS: 4 subjects x 3 layers = 12 rows, all status = SUCCESS,
--       error_message NULL.
-- ============================================================
SELECT data_subject,
       layer,
       status,
       records_inserted,
       records_updated,
       error_records,
       error_message,
       insert_time
FROM   hpe_catalog.audit.job_log
QUALIFY row_number() OVER (PARTITION BY data_subject, layer
                           ORDER BY insert_time DESC) = 1
ORDER  BY data_subject, layer;


-- Any failure ever recorded, newest first.
-- PASS: zero rows (or only failures you already know about and superseded).
SELECT insert_time, data_subject, layer, error_message, batch_id
FROM   hpe_catalog.audit.job_log
WHERE  status <> 'SUCCESS'
ORDER  BY insert_time DESC
LIMIT  50;


-- ============================================================
-- 2. Row counts per layer, per subject.
--
-- Silver holds only current SCD2 versions when filtered on is_active.
-- Gold holds one row per fact grain per frequency.
--
-- PASS: silver_current = gold_rows for every frequency. A gap means the Gold
--       merge dropped or duplicated rows.
-- ============================================================
WITH s AS (
    SELECT _frequency,
           count(*)                                        AS silver_all_versions,
           count_if(is_active)                             AS silver_current
    FROM   hpe_catalog.silver.o9_forecast_ref
    GROUP  BY _frequency
),
g AS (
    SELECT _frequency, count(*) AS gold_rows
    FROM   hpe_catalog.gold.fact_forecast
    GROUP  BY _frequency
)
SELECT coalesce(s._frequency, g._frequency)                AS frequency,
       s.silver_all_versions,
       s.silver_current,
       g.gold_rows,
       g.gold_rows - s.silver_current                      AS drift,
       CASE WHEN g.gold_rows = s.silver_current THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM   s FULL OUTER JOIN g ON s._frequency = g._frequency
ORDER  BY frequency;


-- ============================================================
-- 3. Dimension cardinality.
--
-- The seed data defines 500 products and 200 locations. These two numbers are
-- the fastest proof the seed keying is still correct: if a dimension attribute
-- is keyed to a different expression than the business key, each member churns
-- through attribute values and SCD2 versions it on every load.
--
-- PASS: active_versions = 500 (product) and 200 (location), and
--       distinct_keys matches. Exactly one ACTIVE version per member is the
--       real invariant — that is what the star view joins on.
--
--       total_versions is expected to exceed both. The seed deliberately
--       injects invalid categories (HARDWARE / UNKNOWN / MISC) on a row-keyed
--       cadence, so a product legitimately appears with several categories and
--       SCD2 versions it each time. dim_product around 1,700 total versions is
--       the dirty data doing its job, not a keying fault. dim_location has no
--       such override, which is why it stays at exactly 200 in both columns —
--       and that contrast is the useful signal: if dim_location drifts above
--       200, the location keying really is broken.
-- ============================================================
SELECT 'dim_product'  AS dimension,
       count(*)                                            AS total_versions,
       count_if(is_active)                                 AS active_versions,
       count(DISTINCT product_id)                          AS distinct_keys,
       CASE WHEN count_if(is_active) = 500 THEN 'PASS' ELSE 'FAIL - expected 500' END AS verdict
FROM   hpe_catalog.gold.dim_product
UNION ALL
SELECT 'dim_location',
       count(*),
       count_if(is_active),
       count(DISTINCT location_id),
       CASE WHEN count_if(is_active) = 200 THEN 'PASS' ELSE 'FAIL - expected 200' END
FROM   hpe_catalog.gold.dim_location;


-- Two ACTIVE versions of one member is always a fault: resolve_dim_sk binds
-- facts to the active row, and duplicates there fan the star view out.
-- PASS: zero rows.
SELECT product_id, count(*) AS active_versions
FROM   hpe_catalog.gold.dim_product
WHERE  is_active
GROUP  BY product_id
HAVING count(*) > 1
ORDER  BY active_versions DESC
LIMIT  20;


-- Surrogate keys must be unique across ALL versions, active or expired. The key
-- is sha2(natural_key || effective_from || tracked attributes); before the
-- tracked attributes were mixed in, two versions created on the same day
-- collided, and the star view matched one fact against every colliding row.
-- PASS: zero rows.
SELECT product_sk, count(*) AS rows_sharing_key
FROM   hpe_catalog.gold.dim_product
GROUP  BY product_sk
HAVING count(*) > 1
ORDER  BY rows_sharing_key DESC
LIMIT  20;


-- ============================================================
-- 4. The star view must not fan out.
--
-- v_forecast_star joins the fact to the ACTIVE dimension version only. Without
-- that filter the view double-counts every SCD2 version and overstates every
-- KPI built on it. This is the single most important correctness check in the
-- model, because a fan-out still looks like a working pipeline.
--
-- PASS: star_rows = fact_rows exactly.
-- ============================================================
SELECT (SELECT count(*) FROM hpe_catalog.gold.fact_forecast)   AS fact_rows,
       (SELECT count(*) FROM hpe_catalog.gold.v_forecast_star) AS star_rows,
       CASE WHEN (SELECT count(*) FROM hpe_catalog.gold.v_forecast_star)
               = (SELECT count(*) FROM hpe_catalog.gold.fact_forecast)
            THEN 'PASS' ELSE 'FAIL - view fans out' END       AS verdict;


-- Measures must survive the join unchanged, not just the row count.
-- PASS: both deltas are 0.
SELECT round(f.qty - v.qty, 4)                               AS qty_delta,
       round(f.rev - v.rev, 2)                               AS revenue_delta,
       CASE WHEN round(f.qty - v.qty, 4) = 0
             AND round(f.rev - v.rev, 2) = 0
            THEN 'PASS' ELSE 'FAIL' END                      AS verdict
FROM   (SELECT sum(forecast_qty) qty, sum(revenue_amount) rev
        FROM   hpe_catalog.gold.fact_forecast) f
CROSS JOIN
       (SELECT sum(forecast_qty) qty, sum(revenue_amount) rev
        FROM   hpe_catalog.gold.v_forecast_star) v;


-- ============================================================
-- 5. Referential integrity: no orphaned fact rows.
--
-- resolve_dim_sk binds every fact row to an active dimension version. A NULL
-- on the left join means a fact points at a surrogate key that no active
-- dimension row carries.
--
-- PASS: all three counts are 0.
-- ============================================================
SELECT count_if(p.product_sk  IS NULL)                       AS orphan_product,
       count_if(l.location_sk IS NULL)                       AS orphan_location,
       count_if(t.date_key    IS NULL)                       AS orphan_date,
       CASE WHEN count_if(p.product_sk IS NULL)
               + count_if(l.location_sk IS NULL)
               + count_if(t.date_key IS NULL) = 0
            THEN 'PASS' ELSE 'FAIL' END                      AS verdict
FROM       hpe_catalog.gold.fact_forecast f
LEFT JOIN  hpe_catalog.gold.dim_product  p ON f.product_sk  = p.product_sk AND p.is_active
LEFT JOIN  hpe_catalog.gold.dim_location l ON f.location_sk = l.location_sk AND l.is_active
LEFT JOIN  hpe_catalog.gold.dim_time     t ON f.date_key    = t.date_key;


-- ============================================================
-- 6. Fact grain uniqueness.
--
-- The declared grain is one row per product / location / date / channel /
-- customer / frequency. A duplicate means the merge inserted where it should
-- have updated.
--
-- PASS: zero rows.
-- ============================================================
SELECT product_id, location_id, forecast_date, channel, customer_id, _frequency,
       count(*) AS dupes
FROM   hpe_catalog.gold.fact_forecast
GROUP  BY product_id, location_id, forecast_date, channel, customer_id, _frequency
HAVING count(*) > 1
ORDER  BY dupes DESC
LIMIT  20;


-- Silver must hold exactly one active version per business key.
-- PASS: zero rows.
SELECT product_id, location_id, forecast_date, channel, customer_id, _frequency,
       count(*) AS active_versions
FROM   hpe_catalog.silver.o9_forecast_ref
WHERE  is_active
GROUP  BY product_id, location_id, forecast_date, channel, customer_id, _frequency
HAVING count(*) > 1
ORDER  BY active_versions DESC
LIMIT  20;


-- ============================================================
-- 7. Data quality outcomes.
--
-- NULL_PK and DEDUP quarantine rows. CATEGORY_VALIDATION and
-- CURRENCY_VALIDATION are informational: rows are kept and counted.
--
-- PASS: the domain checks report non-zero records_failed. The seed data
--       deliberately injects bad categories (HARDWARE / UNKNOWN / MISC) and an
--       invalid currency (XYZ), so ZERO failures here means the DQ layer is not
--       running, not that the data is clean.
-- ============================================================
-- Scoped to the LATEST batch per subject. data_quality_log accumulates one row
-- per check per run, so summing unqualified mixes every historical run together
-- and reports a meaningless average. Before 40df1e6 landing accumulated, so run
-- n re-read n copies of the extract and DEDUP correctly rejected n-1 of them --
-- which is why the lifetime quarterly DEDUP rate reads 43.61% against a source
-- that contains no duplicates at all.
WITH latest AS (
    SELECT data_subject, max(insert_time) AS max_ts
    FROM   hpe_catalog.audit.data_quality_log
    GROUP  BY data_subject
),
last_batch AS (
    SELECT DISTINCT d.data_subject, d.batch_id
    FROM   hpe_catalog.audit.data_quality_log d
    JOIN   latest l ON d.data_subject = l.data_subject AND d.insert_time = l.max_ts
)
SELECT d.data_subject,
       d.check_type,
       sum(d.records_checked)                                AS checked,
       sum(d.records_passed)                                 AS passed,
       sum(d.records_failed)                                 AS failed,
       round(100.0 * sum(d.records_passed) / nullif(sum(d.records_checked), 0), 2) AS pass_pct
FROM   hpe_catalog.audit.data_quality_log d
JOIN   last_batch b ON d.data_subject = b.data_subject AND d.batch_id = b.batch_id
GROUP  BY d.data_subject, d.check_type
ORDER  BY d.data_subject, d.check_type;


-- Lifetime view, for comparison. A DEDUP rate well below 100% here with a clean
-- rate above is the accumulating-landing signature, not a data defect.
SELECT data_subject,
       check_type,
       count(DISTINCT batch_id)                              AS runs,
       sum(records_checked)                                  AS checked_all_runs,
       sum(records_failed)                                   AS failed_all_runs
FROM   hpe_catalog.audit.data_quality_log
GROUP  BY data_subject, check_type
ORDER  BY data_subject, check_type;


-- Quarantined rows by reason.
-- PASS: whatever landed here is explained by the seed's deliberate violations.
SELECT _frequency, _dq_fail_reason, count(*) AS rows
FROM   hpe_catalog.silver.quarantine
GROUP  BY _frequency, _dq_fail_reason
ORDER  BY _frequency, _dq_fail_reason;


-- ============================================================
-- 8. Business sanity: no negative or null measures, plausible date span.
--
-- PASS: zero null keys, zero negative measures. Date range should match the
--       seeded full load (Jul 2020 - Jun 2025).
-- ============================================================
SELECT _frequency,
       count(*)                                              AS rows,
       min(forecast_date)                                    AS first_date,
       max(forecast_date)                                    AS last_date,
       count_if(forecast_qty   < 0)                          AS negative_qty,
       count_if(revenue_amount < 0)                          AS negative_revenue,
       count_if(product_id IS NULL OR location_id IS NULL)   AS null_keys,
       count_if(forecast_qty IS NULL)                        AS null_qty
FROM   hpe_catalog.gold.fact_forecast
GROUP  BY _frequency
ORDER  BY _frequency;


-- ============================================================
-- 9. Watermark state.
--
-- After a full load every subject should be 'incremental' with a watermark
-- that sits at the TOP OF THE LOADED DATA, not at the wall-clock time the
-- pipeline ran. A clock value means 00_update_watermark fell back, and the
-- next incremental will filter above every row the source has.
--
-- PASS: last_watermark is close to the source's max change timestamp.
--       A value near updated_ts is a FAIL (stale rows predating 277c105 look
--       like this and must be reset before an incremental run).
-- ============================================================
SELECT data_subject,
       load_type,
       watermark_column,
       last_watermark,
       updated_ts,
       CASE WHEN last_watermark IS NULL              THEN 'no watermark yet'
            WHEN abs(unix_timestamp(try_to_timestamp(last_watermark))
                   - unix_timestamp(updated_ts)) < 900
                 THEN 'SUSPECT - looks like a clock value'
            ELSE 'PASS - derived from data' END               AS verdict
FROM   hpe_catalog.audit.pipeline_config
ORDER  BY data_subject;


-- ============================================================
-- 10. Aggregated audit output.
--
-- 04_aggregated_audit writes ONE TABLE PER SUBJECT:
--   gold.forecast_daily_agg_audit, _weekly_, _monthly_, _quarterly_
-- (the name is derived as data_subject minus its "o9_" prefix), and it appends
-- on every run rather than replacing, so totals accumulate across reloads.
--
-- PASS: every subject present, keyfigure covering forecast_qty and
--       revenue_amount, kpi_value non-null.
-- ============================================================
SELECT 'daily' AS subject, keyfigure, count(*) AS rows,
       sum(no_of_records) AS records_covered, round(sum(kpi_value), 2) AS total
FROM   hpe_catalog.gold.forecast_daily_agg_audit     GROUP BY keyfigure
UNION ALL
SELECT 'weekly', keyfigure, count(*), sum(no_of_records), round(sum(kpi_value), 2)
FROM   hpe_catalog.gold.forecast_weekly_agg_audit    GROUP BY keyfigure
UNION ALL
SELECT 'monthly', keyfigure, count(*), sum(no_of_records), round(sum(kpi_value), 2)
FROM   hpe_catalog.gold.forecast_monthly_agg_audit   GROUP BY keyfigure
UNION ALL
SELECT 'quarterly', keyfigure, count(*), sum(no_of_records), round(sum(kpi_value), 2)
FROM   hpe_catalog.gold.forecast_quarterly_agg_audit GROUP BY keyfigure
ORDER  BY subject, keyfigure;


-- ============================================================
-- 11. Landing is a rolling window, not a pile.
--
-- 01_landing_to_silver archives each file to the archive container and then
-- clears it from landing, so a loaded folder is empty until the next extract.
-- Files left behind mean the clear did not run, and the next cycle will re-read
-- them: the load stays correct (the SCD2 merge dedupes) but grows every run.
--
-- PASS: run this in a notebook cell, not SQL —
--   for p in ["daily", "weekly", "monthly", "quarterly"]:
--       n = len([f for f in dbutils.fs.ls(f"abfss://landing@<acct>.dfs.core.windows.net/o9/{p}/")
--                if f.name.endswith(".parquet")])
--       print(p, n, "PASS" if n == 0 else "files still in landing")
--
-- and confirm the archive side is populated:
--   dbutils.fs.ls("abfss://archive@<acct>.dfs.core.windows.net/o9_forecast_daily/")
-- ============================================================
