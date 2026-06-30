# System Design Document
## HPE o9 Forecast Data Pipeline

**Version:** 1.0  
**Date:** 2026-06-30  

---

## 1. Component Map

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          EXTERNAL SOURCES                                   │
│                                                                             │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────────┐  │
│  │  SAP HANA Cloud  │  │  SQL Server      │  │  Salesforce              │  │
│  │  O9_SOURCE       │  │  HPE_SOURCE.dbo  │  │  Forecast__c             │  │
│  │  .FORECAST_VIEW  │  │  .Forecast       │  │                          │  │
│  │  Port 443 + SSL  │  │  SQLEXPRESS      │  │  login.salesforce.com    │  │
│  └────────┬─────────┘  └────────┬─────────┘  └────────────┬─────────────┘  │
└───────────┼─────────────────────┼────────────────────────┼─────────────────┘
            │  AutoResolve IR     │  shir-onprem           │  AutoResolve IR
            └─────────────────────┴────────────────────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │   Azure Data Factory        │
                    │                             │
                    │  pl_extract_to_landing      │◄── audit.source_extract_metadata
                    │  ├── Lookup (metadata)      │    (Azure SQL)
                    │  ├── ForEach (parallel 4)   │
                    │  │   └── Switch             │
                    │  │       ├── SapHana Copy   │
                    │  │       ├── SqlServer Copy  │
                    │  │       └── Salesforce Copy │
                    │  ├── Update Watermark (SP)  │
                    │  └── Execute pl_master_etl  │
                    └─────────────┬───────────────┘
                                  │  pipe-delimited CSV
                    ┌─────────────▼───────────────┐
                    │   ADLS Gen2                 │
                    │   landing/                  │
                    │   ├── o9/daily/             │
                    │   ├── o9/weekly/            │
                    │   ├── o9/monthly/           │
                    │   └── o9/quarterly/         │
                    └─────────────┬───────────────┘
                                  │
                    ┌─────────────▼───────────────┐
                    │   Azure Data Factory         │
                    │                             │
                    │  pl_master_etl_pipeline     │◄── audit.pipeline_metadata
                    │  ├── Lookup (metadata)      │    (Azure SQL)
                    │  ├── GetMetadata (file?)    │
                    │  └── IfCondition            │
                    │      ├── TRUE → notebooks   │
                    │      └── FALSE → NO_DATA log│
                    └─────────────┬───────────────┘
                                  │  Databricks jobs (sequential)
          ┌───────────────────────┼──────────────────────────┐
          │                       │                          │
          ▼                       ▼                          ▼
   01_ingest_to_bronze    02_bronze_to_silver      03_silver_to_gold
          │                       │                          │
          ▼                       ▼                          ▼
   hpe_catalog.bronze     hpe_catalog.silver       hpe_catalog.gold
   o9_forecast_raw        o9_forecast_ref          o9_forecast_dmnsn
   (Delta, all STRING)    (Delta, typed+clean)     (Delta, star schema)
          │                       │                          │
          ▼                       ▼                          ▼
   bronze.quarantine      silver.dq_log            gold.o9_forecast_agg_audit
   (bad PK rows)          (quality failures)       (KPIs for PowerBI/Tableau)
          │
          └──────────────────────────────────────────────────┐
                                                             │
                                               04_aggregated_audit
                                                             │
                                                             ▼
                                               gold.o9_forecast_agg_audit
                                               (keyfigure/amount rows)

          All notebooks write to ──►  hpe_catalog.audit.job_log (Delta, Unity Catalog)
                                      (batch_id, insert_time, records_inserted,
                                       records_updated, status, layer)
                                      hpe_catalog.audit.data_quality_log (Delta)

          ADF pipelines read from ──► Azure SQL: audit.pipeline_metadata
                                      audit.source_extract_metadata
                                      (config only — not job tracking)
```

---

## 2. Data Flow Detail

### 2.1 Extraction Flow (pl_extract_to_landing)

> **Target state** — current code uses CSV sink and has `LIMIT 200` on Salesforce. Both must be fixed.

```
ADF reads audit.source_extract_metadata
         │
         ├── Row: SAP_HANA / FORECAST_VIEW / incremental / CHANGED_ON
         │   └── Copy: SELECT * FROM O9_SOURCE.FORECAST_VIEW
         │             WHERE CHANGED_ON > '<last_watermark>'
         │             → landing/o9/daily/<timestamp>.parquet   ← Parquet (target)
         │
         ├── Row: SQL_SERVER / Forecast / incremental / modified_dt
         │   └── Copy: SELECT * FROM dbo.Forecast
         │             WHERE modified_dt > '<last_watermark>'
         │             → landing/o9/weekly/<timestamp>.parquet
         │
         ├── Row: SALESFORCE / Forecast__c / full / (none)
         │   └── Copy: SELECT FIELDS(ALL) FROM Forecast__c
         │             (no LIMIT — current LIMIT 200 bug must be removed)
         │             → landing/o9/monthly/<timestamp>.parquet
         │
         └── Row: SALESFORCE / Forecast__c / full / (none)
             └── Copy: SELECT FIELDS(ALL) FROM Forecast__c
                       → landing/o9/quarterly/<timestamp>.parquet
         │
         ▼
  Update watermark in audit.source_extract_metadata (incremental only)
         │
         ▼
  Execute pl_master_etl_pipeline (one call per data_subject)
```

### 2.2 Bronze Flow (01_ingest_to_bronze) — Target Design

> **Responsibility: read, standardize, PK null check, dedup, quarantine, success/failure status**
> Current code issues: reads CSV not Parquet, no standardization, no dedup, no quarantine, PK check wrongly sits in Silver, audit writes to Azure SQL.

```
Read Parquet from landing/o9/<frequency>/
(spark.read.format("parquet"), all columns as STRING, schema-on-read)
         │
         ▼
Add audit columns:
  _file_name    = source filename
  _ingestion_ts = current_timestamp()   ← insert_time per row
  _frequency    = daily/weekly/monthly/quarterly
  _batch_id     = UUID from ADF parameter
         │
         ▼
Standardize (Bronze responsibility):
  TRIM all string columns
  UPPER(category)
  Align column names to common schema across HANA / SQL Server / Salesforce
         │
         ▼
PK null check (product_id, location_id, forecast_date):
  ├── NULL/empty → write to hpe_catalog.bronze.quarantine
  │                with _dq_fail_reason = 'NULL_PK'
  └── Valid      → continue
         │
         ▼
Exact dedup on valid rows:
  ROW_NUMBER() OVER (PARTITION BY product_id, location_id, forecast_date, _frequency
                     ORDER BY _ingestion_ts DESC)
  Keep row_num = 1, drop duplicates
         │
         ▼
Write to hpe_catalog.bronze.o9_forecast_raw (Delta)
  mode = replaceWhere (_frequency = current AND date(_ingestion_ts) = today)
         │
         ▼
Archive Parquet → archive/<frequency>/<timestamp>/
         │
         ▼
Write to hpe_catalog.audit.job_log:
  layer='bronze', records_inserted=valid_count,
  records_updated=0, status=SUCCESS/FAILED
```

### 2.3 Silver Flow (02_bronze_to_silver) — Target Design

> **Responsibility: SCD Type 2, business logic validation, aggregations**
> Current code issues: wrongly does PK check + dedup here (moved to Bronze), no SCD2, no business validation, no aggregations, audit writes to Azure SQL.

```
Read hpe_catalog.bronze.o9_forecast_raw
WHERE _batch_id = current_batch_id
(Bronze already guarantees: no NULL PKs, no exact duplicates)
         │
         ▼
Type casting:
  forecast_date  → DATE
  forecast_qty   → DECIMAL(12,2)
  revenue_amount → DECIMAL(16,2)
         │
         ▼
Business logic validation (flag violations, do NOT drop rows):
  category NOT IN VALID_CATEGORIES → log to audit.data_quality_log
  currency NOT IN VALID_CURRENCIES → log to audit.data_quality_log
         │
         ▼
SCD Type 2 MERGE into hpe_catalog.silver.o9_forecast_ref:
  MATCH on (product_id, location_id, forecast_date, _frequency)

  WHEN MATCHED AND business values changed:
    UPDATE old row: effective_to = today, is_active = false
    INSERT new row: effective_from = today, effective_to = NULL, is_active = true

  WHEN MATCHED AND no change:
    skip (idempotent — no duplicate insert)

  WHEN NOT MATCHED:
    INSERT: effective_from = today, effective_to = NULL, is_active = true
         │
         ▼
Aggregations:
  GROUP BY (category, region, period, _frequency)
  SUM(forecast_qty), SUM(revenue_amount)
  → append to hpe_catalog.silver.o9_forecast_period_agg
         │
         ▼
Write to hpe_catalog.audit.job_log:
  layer='silver', records_inserted=new rows,
  records_updated=SCD2 updates, status=SUCCESS/FAILED
```

### 2.4 Gold Flow (03_silver_to_gold) — Target Design

> **Current code issues:** `mode("overwrite")` destroys history, dim tables never populated, dead code bug (dangling write at lines 65–76), archive logic missing, KPI filter uses date instead of batch_id.

```
Read hpe_catalog.silver.o9_forecast_ref
WHERE is_active = true AND _batch_id = current_batch_id
         │
         ▼
Derive period: YYYY-MM-01 from forecast_date
         │
         ▼
Populate dimension tables (SCD Type 2 MERGE):
  dim_product:  MERGE on product_id — track category/sub_category changes
  dim_location: MERGE on location_id — track region/country changes
  dim_time:     INSERT missing date_keys derived from forecast_date
         │
         ▼
Build fact rows: join silver → surrogate keys from dims
         │
         ▼
MERGE into hpe_catalog.gold.fact_o9_forecast:
  MATCH on (product_key, location_key, time_key, channel, _frequency)
  WHEN NOT MATCHED → INSERT   (new period record)
  WHEN MATCHED     → no update (historical periods are immutable)
         │
         ▼
Archive Silver partition → archive/silver/<period>/
  (move processed _ingestion_date partition after Gold load)
         │
         ▼
Write to hpe_catalog.audit.job_log:
  layer='gold', records_inserted=<new fact rows>, records_updated=0
```

### 2.5 KPI Aggregation (04_aggregated_audit) — Target Design

> **Current code issue:** filters `_gold_load_ts = today` — breaks on reruns. Must filter by `_batch_id` instead.

```
Read hpe_catalog.gold.fact_o9_forecast
WHERE _batch_id = current_batch_id   ← use batch_id not date
         │
         ▼
GROUP BY period, category, region, _frequency
SUM(forecast_qty), SUM(revenue_amount)
         │
         ▼
Transpose (stack):
  (period, category, region, keyfigure='forecast_qty',   total=SUM(forecast_qty))
  (period, category, region, keyfigure='revenue_amount', total=SUM(revenue_amount))
         │
         ▼
Append to hpe_catalog.gold.o9_forecast_agg_audit
(source for PowerBI / Tableau dashboards)
```

---

## 3. Database Schemas

### 3.1 Azure SQL — audit-db (pipeline configuration only)

```sql
audit.pipeline_metadata        -- one row per data_subject (runtime config)
  data_subject, source_system, source_path, bronze_table,
  silver_table, gold_table, frequency, is_active

audit.source_extract_metadata  -- one row per source object (extraction config + watermark)
  source_system, connector, source_schema, source_object,
  landing_path, data_subject, load_type, watermark_column,
  last_watermark, is_active
```

> Azure SQL holds **configuration only**. Job tracking and DQ logs live in Unity Catalog (see 3.2 below).

### 3.2 Unity Catalog — hpe_catalog.audit (job tracking + DQ)

```sql
hpe_catalog.audit.job_log      -- one row per notebook run per layer
  batch_id        STRING        -- UUID from ADF, links all layers in one run
  insert_time     TIMESTAMP     -- when the row was written
  layer           STRING        -- bronze / silver / gold / agg_audit
  status          STRING        -- SUCCESS / FAILED
  records_inserted BIGINT       -- new rows written
  records_updated  BIGINT       -- rows updated (SCD2 merges in Silver/Gold)
  source_system   STRING
  data_subject    STRING
  error_message   STRING        -- NULL on success

hpe_catalog.audit.data_quality_log   -- one row per DQ check per run
  batch_id, table_name, check_type,
  records_checked, records_passed, records_failed, check_ts
```

### 3.2 Unity Catalog — hpe_catalog

```
hpe_catalog
├── bronze
│   ├── o9_forecast_raw      (all STRING + audit cols, Delta)
│   └── quarantine           (failed PK rows, Delta)
├── silver
│   └── o9_forecast_ref      (typed cols, NOT NULL PKs, Delta)
└── gold
    ├── o9_forecast_dmnsn    (periodic fact, partitioned by period+frequency)
    ├── o9_forecast_agg_audit (KPI summary, keyfigure rows)
    ├── dim_product           (SCD Type 2)
    ├── dim_location          (SCD Type 2)
    └── dim_time              (derived period attributes)
```

---

## 4. ADF Pipeline Interactions

```
Trigger (daily/weekly/monthly/quarterly schedule)
    │
    ▼
pl_extract_to_landing
    │  reads  → audit.source_extract_metadata (Azure SQL)
    │  writes → landing/o9/<frequency>/ (ADLS CSV)
    │  calls  → audit.usp_update_extract_watermark (Azure SQL SP)
    │  calls  → pl_master_etl_pipeline (ADF Execute Pipeline)
    │
    ▼
pl_master_etl_pipeline
    │  reads  → audit.pipeline_metadata (Azure SQL Lookup)
    │  checks → landing file existence (GetMetadata)
    │  runs   → 01_ingest_to_bronze (Databricks notebook)
    │  runs   → 02_bronze_to_silver (Databricks notebook)
    │  runs   → 03_silver_to_gold   (Databricks notebook)
    │  runs   → 04_aggregated_audit (Databricks notebook)
    │  each notebook writes → audit.pipeline_audit (JDBC)
```

---

## 5. Integration Runtime Routing

| Source | IR Type | Reason |
|---|---|---|
| SAP HANA Cloud | AutoResolve (Azure IR) | Public internet endpoint (port 443) |
| SQL Server on-prem | `shir-onprem` (Self-Hosted IR) | Behind corporate network, no public endpoint |
| Salesforce | AutoResolve (Azure IR) | SaaS, public internet |
| ADLS Gen2 | AutoResolve (Azure IR) | Native Azure service |
| Azure SQL | AutoResolve (Azure IR) | Native Azure service |
| Databricks | AutoResolve (Azure IR) | Native Azure service |

---

## 6. Key Vault Secret Routing

```
Azure Key Vault (kv-hpe-o9-dev)
    │
    ├── saphana-password          → ls_sap_hana (ADF linked service)
    ├── sqlserver-password        → ls_sql_server (ADF linked service)
    ├── salesforce-password       → ls_salesforce (ADF linked service)
    ├── salesforce-security-token → ls_salesforce (ADF linked service)
    ├── adls-access-key           → ls_adls_gen2 + Databricks secret scope
    ├── sql-user                  → 00_config notebook (JDBC connection)
    └── sql-password              → 00_config notebook (JDBC connection)
```

---

## 7. Error Handling

| Failure Point | Behaviour | Recovery |
|---|---|---|
| Source extract fails | ADF activity fails; ForEach continues other sources | Re-run pl_extract_to_landing for failed source |
| Landing file missing | pl_master_etl_pipeline IfCondition = FALSE → logs NO_DATA | Drop CSV manually or re-run extract |
| Bronze PK null rows | Routed to `bronze.quarantine`, not dropped | Investigate quarantine table, fix source, reprocess |
| Silver DQ failures | Logged to `audit.data_quality_log` | Query DQ log, fix source data, re-run Silver |
| Notebook exception | ADF marks notebook activity FAILED; audit entry written with FAILED status | Check `audit.pipeline_audit` for error_message |
| Watermark not updated | Next incremental run re-extracts from old watermark (duplicate rows) | Silver dedup logic handles re-ingested duplicates |
