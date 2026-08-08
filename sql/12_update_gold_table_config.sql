-- ============================================================
-- Point pipeline_config at the star-schema fact table.
--
-- The Gold layer was rebuilt as a star schema: gold.o9_forecast_dmnsn (a flat
-- table duplicating dimension attributes) was replaced by gold.fact_forecast
-- plus conformed dimensions. 05_pipeline_config.sql was updated for fresh
-- deployments, but an already-deployed config table still holds the old name.
--
-- 03_silver_to_gold does not read this column - it targets fact_forecast
-- directly - so this is a correctness fix for the metadata rather than a
-- blocker. It matters once ADF or any other consumer reads gold_table.
--
-- Idempotent: re-running changes nothing once applied.
-- ============================================================

UPDATE hpe_catalog.audit.pipeline_config
SET    gold_table = 'hpe_catalog.gold.fact_forecast'
WHERE  gold_table = 'hpe_catalog.gold.o9_forecast_dmnsn';

-- ── Verify: all 4 subjects should show the fact table ─────────
SELECT data_subject, frequency, bronze_table, silver_table, gold_table
FROM   hpe_catalog.audit.pipeline_config
WHERE  is_active = true
ORDER  BY data_subject;

-- ── Optional cleanup ──────────────────────────────────────────
-- The pre-star table, if it was ever created. Superseded by fact_forecast.
-- DROP TABLE IF EXISTS hpe_catalog.gold.o9_forecast_dmnsn;
