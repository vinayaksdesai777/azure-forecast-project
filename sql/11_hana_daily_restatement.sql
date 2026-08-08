-- ============================================================
-- SAP HANA — daily forecast RESTATEMENT (Case A)
--
-- Every other delta script (07/08/09) emits new forecast_dates, so the merge
-- keys never collide and every row is a plain insert. Nothing exercises the
-- restatement path, even though CHANGED_ON / LastModifiedDate exist precisely
-- to capture it.
--
-- This script revises forecast_qty and revenue_amount for rows that ALREADY
-- exist, and advances CHANGED_ON so ADF's watermark picks them up. The
-- business keys are unchanged, so:
--
--   Silver : expires the old version, inserts the revised one (SCD2)
--   Gold   : replaceWhere rebuilds the frequency slice, so the fact shows
--            the revised value rather than the stale one
--
-- Run AFTER 07_hana_daily_delta.sql. Re-runnable: each execution revises the
-- same rows again with a later CHANGED_ON.
-- ============================================================

-- ── Revise one month of existing daily forecasts ──────────────
-- Uplift of 15% on quantity and revenue, simulating a forecast revision.
UPDATE "O9_SOURCE"."FORECAST_VIEW"
SET
    "FORECAST_QTY"   = ROUND("FORECAST_QTY"   * 1.15, 4),
    "REVENUE_AMOUNT" = ROUND("REVENUE_AMOUNT" * 1.15, 2),
    "CHANGED_ON"     = ADD_SECONDS(CURRENT_TIMESTAMP, 0)
WHERE "FORECAST_DATE" BETWEEN '2026-03-01' AND '2026-03-31';

-- ── Verify the restatement ────────────────────────────────────
-- Row count must be unchanged: these are revisions, not new rows.
SELECT
    COUNT(*)              AS revised_rows,
    MIN("FORECAST_DATE")  AS from_date,
    MAX("FORECAST_DATE")  AS to_date,
    MAX("CHANGED_ON")     AS new_watermark
FROM "O9_SOURCE"."FORECAST_VIEW"
WHERE "FORECAST_DATE" BETWEEN '2026-03-01' AND '2026-03-31';

-- ── Expected downstream behaviour ─────────────────────────────
-- After re-running 01 -> 02 -> 03 for o9_forecast_daily:
--
--   silver.o9_forecast_ref
--     the revised keys have 2 rows: is_active = false (old qty) and
--     is_active = true (new qty)
--
--   gold.fact_forecast
--     ONE row per grain, carrying the revised qty. Before the replaceWhere
--     change this showed the stale value, because an insert-only MERGE
--     matched the existing key and dropped the update.
--
-- Check with:
--   SELECT is_active, SUM(forecast_qty) FROM hpe_catalog.silver.o9_forecast_ref
--   WHERE forecast_date BETWEEN '2026-03-01' AND '2026-03-31' GROUP BY is_active;
--
--   SELECT SUM(forecast_qty) FROM hpe_catalog.gold.fact_forecast
--   WHERE forecast_date BETWEEN '2026-03-01' AND '2026-03-31';
--
-- The Gold sum must equal the Silver is_active = true sum.
