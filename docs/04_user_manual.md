# User Manual
## HPE o9 Forecast Data Pipeline — Operations Guide

**Version:** 1.0  
**Date:** 2026-06-30  
**Audience:** Data Engineers, Pipeline Operators

---

## 1. Overview

This guide explains how to operate the HPE o9 Forecast Data Pipeline day-to-day: how to trigger runs, monitor progress, investigate failures, add new data sources, and interpret audit logs.

The pipeline runs automatically on schedule (daily/weekly/monthly/quarterly). This guide covers what to do when something goes wrong or when you need to run things manually.

---

## 2. Prerequisites

Before operating the pipeline you need:

| Requirement | Where to get it |
|---|---|
| Azure Portal access | Azure subscription — Contributor role on `rg-hpe-data-pipeline` |
| ADF Studio access | Portal → Data Factories → `adf-hpe-o9-dev` → Author & Monitor |
| Databricks workspace access | `dbw-hpe-o9-dev.azuredatabricks.net` |
| Azure SQL access | `sql-hpe-o9-dev.database.windows.net` / `audit-db` — read access to `audit` schema |
| SSMS or Azure Data Studio | For querying the audit database |

---

## 3. Normal Pipeline Operations

### 3.1 How the Pipeline Runs Automatically

Four ADF triggers fire on schedule:

| Trigger | Frequency | What it processes |
|---|---|---|
| `trigger_daily` | Every day at 06:00 UTC | SAP HANA → `o9_forecast_daily` |
| `trigger_weekly` | Every Monday at 07:00 UTC | SQL Server → `o9_forecast_weekly` |
| `trigger_monthly` | 1st of each month at 08:00 UTC | Salesforce monthly → `o9_forecast_monthly` |
| `trigger_quarterly` | 1st of Jan/Apr/Jul/Oct at 08:00 UTC | Salesforce quarterly → `o9_forecast_quarterly` |

Each trigger calls `pl_extract_to_landing` → which calls `pl_master_etl_pipeline` → which runs Bronze → Silver → Gold → Audit notebooks in sequence.

### 3.2 Check if the Pipeline Ran Successfully

**Option A — ADF Monitor (quickest):**
1. Open ADF Studio → **Monitor** (left sidebar)
2. Click **Pipeline runs**
3. Filter by pipeline name: `pl_extract_to_landing`
4. Green tick = success. Red X = failure.

**Option B — Audit database query:**
```sql
-- Last 7 days of runs, one row per layer per run
SELECT batch_id, data_layer, job_status, source_row_count,
       target_row_count, error_record_count, job_start_ts, job_end_ts
FROM audit.pipeline_audit
WHERE job_start_ts >= DATEADD(DAY, -7, GETUTCDATE())
ORDER BY job_start_ts DESC;
```

**What good looks like:**
```
batch_id  | data_layer    | job_status | source_rows | target_rows | error_rows
----------+---------------+------------+-------------+-------------+-----------
abc-123   | bronze        | SUCCESS    | 9800        | 9800        | 0
abc-123   | silver        | SUCCESS    | 9800        | 9450        | 350
abc-123   | gold          | SUCCESS    | 9450        | 9450        | 0
abc-123   | agg_audit     | SUCCESS    | 9450        | 8           | 0
```

Note: Silver target rows < source rows is **normal** — that's dirty rows being quarantined/filtered.

---

## 4. Manually Triggering a Pipeline Run

Use this when you need to re-run a failed pipeline or backfill data.

### 4.1 Trigger via ADF Studio
1. Open ADF Studio → **Author** → **Pipelines**
2. Click `pl_extract_to_landing`
3. Click **Add trigger** → **Trigger now**
4. Enter parameters if prompted (data_subject, storage_account)
5. Click **OK**

### 4.2 Trigger via ADF Monitor (re-run failed)
1. Open **Monitor** → **Pipeline runs**
2. Find the failed run
3. Click the three-dot menu → **Rerun**

### 4.3 Trigger just one layer (Bronze/Silver/Gold only)

If extraction already succeeded but a Databricks notebook failed:
1. Open `pl_master_etl_pipeline`
2. Click **Add trigger** → **Trigger now**
3. Enter `data_subject` (e.g. `o9_forecast_daily`) and `storage_account`
4. This skips extraction and goes straight to Bronze

---

## 5. Monitoring Data Quality

### 5.1 Check DQ Log After a Run
```sql
-- DQ results for the latest batch
SELECT batch_id, table_name, check_type,
       records_checked, records_passed, records_failed,
       CAST(records_passed * 100.0 / records_checked AS DECIMAL(5,2)) AS pass_rate_pct
FROM audit.data_quality_log
WHERE batch_id = '<paste batch_id here>'
ORDER BY check_type;
```

### 5.2 Interpret DQ Results

| check_type | What it means | Action if records_failed > 0 |
|---|---|---|
| `null_check` | Rows with NULL primary keys (product_id, location_id, forecast_date) | Check `bronze.quarantine` — investigate source data |
| `dedup_check` | Duplicate rows removed | Normal if source sends deltas with overlapping windows |
| `type_cast` | Columns that couldn't cast to expected type | Source schema may have changed — check source system |
| `currency_validation` | Invalid currency codes (e.g. `XX`) | Silver DQ log — fix in source or map in Silver logic |
| `category_validation` | Invalid category values | Fix in source or add to allowed-values list in Silver |

### 5.3 Check Quarantine Table
```sql
-- Rows that failed Bronze PK check
SELECT * FROM hpe_catalog.bronze.quarantine
WHERE _batch_id = '<batch_id>'
ORDER BY _ingestion_ts DESC
LIMIT 100;
```

---

## 6. Investigating Pipeline Failures

### 6.1 Step-by-Step Failure Investigation

**Step 1 — Find the failed run in ADF Monitor**
- Note the `batch_id` from the pipeline run details (under Parameters).

**Step 2 — Query audit table for error details**
```sql
SELECT batch_id, data_layer, job_status, error_record_count,
       job_start_ts, job_end_ts
FROM audit.pipeline_audit
WHERE batch_id = '<batch_id>';
```

**Step 3 — Find which layer failed**
- If `bronze` = FAILED → check Databricks notebook `01_ingest_to_bronze` logs
- If `silver` = FAILED → check `02_bronze_to_silver` logs
- If `gold` = FAILED → check `03_silver_to_gold` logs

**Step 4 — Check Databricks notebook logs**
1. Open Databricks workspace
2. Click **Workflows** → **Job runs** (or check cluster logs)
3. Find the run by timestamp
4. Click the failed task → view stdout / stderr

**Step 5 — Common error messages and fixes**

| Error | Cause | Fix |
|---|---|---|
| `AnalysisException: Table not found` | Unity Catalog table doesn't exist yet | Run DDL scripts from `data_model/` |
| `JDBC connection refused` | Azure SQL firewall blocking Databricks IP | Add Databricks outbound IP to Azure SQL firewall |
| `403 Forbidden on ADLS` | Databricks service principal lacks Storage Blob Data Contributor | Grant role in Azure Portal → ADLS → IAM |
| `AuthTimeoutError` in ADF linked service | Key Vault secret expired or wrong name | Check Key Vault → verify secret name matches linked service |
| `No files found in landing` | Extract pipeline didn't run or failed | Check `pl_extract_to_landing` run status first |
| `Watermark not updated` | `usp_update_extract_watermark` SP failed | Check Azure SQL — re-run SP manually if needed |

---

## 7. Adding a New Data Source

To add a new source without changing pipeline code:

**Step 1 — Add extract metadata row**
```sql
INSERT INTO audit.source_extract_metadata
    (source_system, connector, source_schema, source_object,
     landing_path, data_subject, load_type, watermark_column, last_watermark, is_active)
VALUES
    ('NEW_SOURCE', 'SqlServer', 'dbo', 'NewTable',
     'o9/new_path/', 'o9_new_subject', 'incremental', 'modified_dt',
     '1900-01-01 00:00:00', 1);
```

**Step 2 — Add pipeline metadata row**
```sql
INSERT INTO audit.pipeline_metadata
    (data_subject, source_system, source_path, bronze_table,
     silver_table, gold_table, frequency, is_active)
VALUES
    ('o9_new_subject', 'NEW_SOURCE', 'o9/new_path/',
     'hpe_catalog.bronze.o9_new_raw',
     'hpe_catalog.silver.o9_new_ref',
     'hpe_catalog.gold.o9_new_dmnsn',
     'weekly', 1);
```

**Step 3 — Create Delta tables**
Run the DDL in Databricks SQL editor to create the bronze/silver/gold tables for the new subject.

**Step 4 — Add Key Vault secret** (if new source needs new credentials)
```powershell
az keyvault secret set --vault-name "kv-hpe-o9-dev" --name "newsource-password" --value "<password>"
```

**Step 5 — Add ADF linked service + dataset** for the new source connector type (if not already covered by SapHana / SqlServer / Salesforce switch).

---

## 8. Watermark Management

Watermarks control incremental extraction for SAP HANA and SQL Server.

### 8.1 Check current watermarks
```sql
SELECT source_system, source_object, watermark_column, last_watermark, updated_ts
FROM audit.source_extract_metadata
WHERE load_type = 'incremental';
```

### 8.2 Reset watermark (full reload)
Use this when you need to re-extract all historical data:
```sql
UPDATE audit.source_extract_metadata
SET last_watermark = '1900-01-01 00:00:00',
    updated_ts = GETUTCDATE()
WHERE source_system = 'SAP_HANA'
  AND source_object = 'FORECAST_VIEW';
```

**Warning:** Resetting to 1900 will re-extract all rows. Silver dedup will handle duplicates, but the extract may be slow and produce a large landing file.

### 8.3 Manually set watermark to a specific date
```sql
UPDATE audit.source_extract_metadata
SET last_watermark = '2025-01-01 00:00:00',
    updated_ts = GETUTCDATE()
WHERE source_system = 'SQL_SERVER'
  AND source_object = 'Forecast';
```

---

## 9. Unity Catalog — Access Management

### 9.1 Grant a user read access to Gold tables
```sql
-- In Databricks SQL editor
GRANT SELECT ON hpe_catalog.gold.o9_forecast_dmnsn TO `user@domain.com`;
GRANT SELECT ON hpe_catalog.gold.o9_forecast_agg_audit TO `user@domain.com`;
```

### 9.2 Grant a service principal write access to a layer
```sql
GRANT ALL PRIVILEGES ON SCHEMA hpe_catalog.bronze TO `sp-adf-pipeline`;
```

### 9.3 Check who has access
```sql
SHOW GRANTS ON hpe_catalog.gold.o9_forecast_dmnsn;
```

---

## 10. Useful Monitoring Queries

```sql
-- 1. Today's pipeline run summary
SELECT * FROM audit.vw_latest_pipeline_runs
WHERE CAST(job_start_ts AS DATE) = CAST(GETUTCDATE() AS DATE);

-- 2. Data quality pass rates by table
SELECT * FROM audit.vw_dq_summary ORDER BY check_ts DESC;

-- 3. Row volume trend (last 30 days)
SELECT CAST(job_start_ts AS DATE) AS run_date,
       data_layer,
       SUM(target_row_count) AS rows_loaded
FROM audit.pipeline_audit
WHERE job_start_ts >= DATEADD(DAY, -30, GETUTCDATE())
  AND job_status = 'SUCCESS'
GROUP BY CAST(job_start_ts AS DATE), data_layer
ORDER BY run_date DESC, data_layer;

-- 4. Failed runs in last 7 days
SELECT batch_id, data_layer, job_start_ts, error_record_count
FROM audit.pipeline_audit
WHERE job_status = 'FAILED'
  AND job_start_ts >= DATEADD(DAY, -7, GETUTCDATE())
ORDER BY job_start_ts DESC;

-- 5. Salesforce full reload check
SELECT source_system, last_watermark, updated_ts
FROM audit.source_extract_metadata
WHERE source_system = 'SALESFORCE';
-- last_watermark will be NULL for full-load sources
```

---

## 11. Quick Reference

| Task | Where |
|---|---|
| Check if pipeline ran | ADF Monitor → Pipeline runs |
| See row counts per layer | `SELECT * FROM audit.vw_latest_pipeline_runs` |
| See DQ failures | `SELECT * FROM audit.data_quality_log WHERE batch_id='...'` |
| See quarantined rows | `SELECT * FROM hpe_catalog.bronze.quarantine` |
| Reset watermark | `UPDATE audit.source_extract_metadata SET last_watermark=...` |
| Manually trigger pipeline | ADF Studio → pl_extract_to_landing → Trigger now |
| Add new source | Insert into `source_extract_metadata` + `pipeline_metadata` |
| Grant table access | Databricks SQL: `GRANT SELECT ON ... TO user` |
| Check Key Vault secrets | Azure Portal → kv-hpe-o9-dev → Secrets |
