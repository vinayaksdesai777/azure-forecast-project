# User Manual
## o9 Forecast Data Pipeline — Operations Guide

**Status:** as-built
**Audience:** whoever is on call for this pipeline

---

## 1. Overview

Four data subjects run on four independent schedules through the same two ADF pipelines
and the same Databricks notebooks. Everything you need to answer "did it work?" is in
`hpe_catalog.audit.job_log`.

| Subject | Source | Schedule (UTC) |
|---|---|---|
| `o9_forecast_daily` | SAP HANA Cloud | 06:00 daily |
| `o9_forecast_weekly` | SQL Server (on-prem) | 07:00 Mondays |
| `o9_forecast_monthly` | Salesforce | 08:00 on the 1st |
| `o9_forecast_quarterly` | SAP HANA Cloud | 09:00 quarterly |

Flow: `pl_extract_to_landing` (source → landing Parquet, watermark) →
`pl_master_etl_pipeline` (`01_landing_to_silver` → `03_silver_to_gold` →
`04_aggregated_audit`).

There is no Bronze layer. Landing Parquet plus the archive container is the raw archive.

---

## 2. Prerequisites

- Access to the Azure Data Factory instance (Monitor at minimum)
- Access to the Databricks workspace
- `SELECT` on `hpe_catalog.audit.*` to run the queries below
- For on-prem issues: access to the machine hosting `shir-hpe-forecast`

---

## 3. Did the pipeline run?

### 3.1 Quick check

```sql
SELECT data_subject, layer, status, records_inserted, records_updated,
       error_records, insert_time
FROM   hpe_catalog.audit.job_log
WHERE  insert_time >= current_date() - INTERVAL 1 DAY
ORDER  BY insert_time DESC;
```

A healthy subject shows three rows — `silver`, `gold`, `agg_audit` — all `SUCCESS`,
sharing one `run_id`.

### 3.2 One specific run

```sql
SELECT layer, status, records_inserted, records_updated, error_records, error_message
FROM   hpe_catalog.audit.job_log
WHERE  run_id = '<adf-run-id>'
ORDER  BY insert_time;
```

Copy the run id from the ADF Monitor blade.

### 3.3 Reading the result

| What you see | Means |
|---|---|
| Three `SUCCESS` rows | Normal |
| `SUCCESS` with all counts zero | Source had nothing new. Not a failure |
| `silver` `SUCCESS`, no `gold` row | Gold failed to start — check ADF activity output |
| `FAILED` with `error_message` | Read the message; see §6 |
| No rows at all | The trigger never fired, or extraction failed before Silver |

Batch ids are stage-prefixed (`silver_…`, `gold_…`, `agg_…`), so a row names its own
layer without a join.

---

## 4. Triggering a run manually

### 4.1 Full run for one subject

ADF Studio → Author → `pl_extract_to_landing` → **Debug**. It reads config, so it picks
up whichever subjects are `is_active = true`.

To run a single subject, temporarily set the others inactive:

```sql
UPDATE hpe_catalog.audit.pipeline_config
SET    is_active = false, updated_ts = current_timestamp()
WHERE  data_subject <> 'o9_forecast_monthly';
```

Set them back to `true` afterwards.

### 4.2 Re-run a failed run

ADF Monitor → find the run → **Rerun from failed activity**. Safe: Silver merges and
Gold's `replaceWhere` are both idempotent.

### 4.3 Run one notebook by hand

Open the notebook in Databricks and set the widgets:

| Notebook | Widgets |
|---|---|
| `01_landing_to_silver` | `data_subject`, optional `source_path` override |
| `03_silver_to_gold` | `data_subject` |
| `04_aggregated_audit` | `data_subject`, `batch_id` |

For `04_aggregated_audit`, `batch_id` must be the one returned by `03_silver_to_gold`
(prefixed `gold_…`). Passing Silver's batch id makes it fail with a list of the batch ids
actually present — that failure is intentional.

---

## 5. Monitoring data quality

### 5.1 Check the DQ log

```sql
SELECT check_type, column_name, records_checked, records_passed, records_failed,
       round(100.0 * records_passed / nullif(records_checked, 0), 2) AS pass_pct
FROM   hpe_catalog.audit.data_quality_log
WHERE  batch_id = '<batch-id>'
ORDER  BY check_type;
```

### 5.2 Interpreting it

| Check | Rows dropped? | Action |
|---|---|---|
| `NULL_PK` | **Yes** — quarantined | Investigate if the count jumps; a null business key is unrecoverable |
| `DEDUP` | **Yes** — latest kept per key | Normal in small numbers; a spike suggests a double extract |
| `CATEGORY_VALIDATION` | No — logged only | Often a legitimately new category. Add it to `VALID_CATEGORIES` in `00_config.py` if so |
| `CURRENCY_VALIDATION` | No — logged only | Same. Add to `VALID_CURRENCIES` if legitimate |

The seed data deliberately injects `HARDWARE`, `UNKNOWN`, `MISC` categories and an `XYZ`
currency, so non-zero domain failures are expected on seeded data.

### 5.3 Inspect quarantined rows

```sql
SELECT * FROM hpe_catalog.silver.quarantine
WHERE  _batch_id = '<batch-id>'
LIMIT  50;
```

`_dq_fail_reason` says which check rejected the row.

---

## 6. Investigating failures

1. **Find the failing layer.**
   ```sql
   SELECT layer, object_name, error_message, insert_time
   FROM   hpe_catalog.audit.job_log
   WHERE  status = 'FAILED'
   ORDER  BY insert_time DESC LIMIT 20;
   ```
   The audit row is written *before* the exception re-raises, so it always exists.

2. **Read `error_message` first.** It is the actual exception text.

3. **Open the ADF activity output** for the stack trace and the Databricks run URL.

4. **Common causes:**

| Symptom | Cause | Fix |
|---|---|---|
| `No active pipeline_config row for: <subject>` | Subject inactive or misspelled | Check `pipeline_config` |
| `batch_id '…' matched 0 rows` | Wrong batch id into `04_aggregated_audit` | Pass Gold's batch id, not Silver's |
| HANA connection / TLS error | `ngdbc.jar` missing, or cluster not Single User | Reinstall the JAR; Shared clusters are blocked by the UC artifact allowlist |
| SQL Server timeout | SHIR offline | Check the SHIR service on the on-prem host |
| Salesforce storage error | Dev org 10 MB cap | Expected; partial load is handled correctly |
| `Path does not exist` on landing | Extraction never wrote | Check `pl_extract_to_landing` ran first |
| Dimension counts drifting above 500/200 | Seed data keying regression | See §9 |

---

## 7. Adding a new data subject

No code change is required.

1. Insert a row into `hpe_catalog.audit.pipeline_config` with `data_subject`,
   `source_system`, `connector`, `source_object`, `frequency`, `landing_path`,
   `silver_table`, `gold_table`, `load_type='full'`, `watermark_column`,
   `null_check_columns`, `partition_column`, `is_active=true`.
2. If the source system is new, add its linked service and dataset in ADF and a `Switch`
   branch on `connector` in `pl_extract_to_landing`.
3. Create a trigger for its schedule.
4. Run once with `load_type='full'`, confirm `job_log`, then let the watermark flip it to
   `incremental`.

---

## 8. Watermark management

```sql
-- Current watermarks
SELECT data_subject, load_type, watermark_column, last_watermark, updated_ts
FROM   hpe_catalog.audit.pipeline_config
ORDER  BY data_subject;

-- Force a full reload of one subject
UPDATE hpe_catalog.audit.pipeline_config
SET    load_type = 'full', last_watermark = NULL, updated_ts = current_timestamp()
WHERE  data_subject = '<subject>';

-- Set a specific watermark
UPDATE hpe_catalog.audit.pipeline_config
SET    last_watermark = '2026-01-31 23:59:59', updated_ts = current_timestamp()
WHERE  data_subject = '<subject>';
```

`00_update_watermark` does this automatically after a successful extract.

---

## 9. Health checks worth running

```sql
-- Conformed dimensions must stay at these exact counts
SELECT count(*) FROM hpe_catalog.gold.dim_product;    -- expect 500
SELECT count(*) FROM hpe_catalog.gold.dim_location;   -- expect 200

-- The star view must not fan out
SELECT (SELECT count(*) FROM hpe_catalog.gold.v_forecast_star) AS star_rows,
       (SELECT count(*) FROM hpe_catalog.gold.fact_forecast)   AS fact_rows;
```

If `dim_product` climbs above 500, a dimension attribute is changing per row rather than
per product, and Gold is versioning the member on every load. That was a real defect
(fixed in `13c81a3`): dimension attributes must derive from the same expression that
builds the key. Check the seed scripts before suspecting the pipeline.

---

## 10. Access management

```sql
-- Analyst: Gold only
GRANT USAGE  ON CATALOG hpe_catalog        TO `analyst-group`;
GRANT USAGE  ON SCHEMA  hpe_catalog.gold   TO `analyst-group`;
GRANT SELECT ON SCHEMA  hpe_catalog.gold   TO `analyst-group`;

-- Who has access
SHOW GRANTS ON SCHEMA hpe_catalog.gold;
```

Analysts should not be granted Silver — it holds quarantined and superseded SCD2 rows
that will double-count in naive queries.

---

## 11. Quick reference

| I want to… | Do this |
|---|---|
| See if last night ran | §3.1 |
| Debug a failure | §6 |
| Re-run safely | ADF Monitor → Rerun from failed activity |
| Check DQ | §5.1 |
| Force a full reload | §8 |
| Add a source | §7 |
| Reset the medallion | `sql/10_reset_medallion.sql`, then `data_model/03_gold_schema.sql` |
| Verify a clean load | §9 |

**Key tables:** `hpe_catalog.audit.job_log`, `hpe_catalog.audit.data_quality_log`,
`hpe_catalog.audit.pipeline_config`, `hpe_catalog.silver.o9_forecast_ref`,
`hpe_catalog.gold.fact_forecast`, `hpe_catalog.gold.v_forecast_star`.
