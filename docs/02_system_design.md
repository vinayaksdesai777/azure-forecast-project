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

          All notebooks write to ──►  Azure SQL: audit.pipeline_audit
                                      (batch_id, status, row counts, layer)
```

---

## 2. Data Flow Detail

### 2.1 Extraction Flow (pl_extract_to_landing)

```
ADF reads audit.source_extract_metadata
         │
         ├── Row: SAP_HANA / FORECAST_VIEW / incremental / CHANGED_ON
         │   └── Copy: SELECT * FROM O9_SOURCE.FORECAST_VIEW
         │             WHERE CHANGED_ON > '<last_watermark>'
         │             → landing/o9/daily/<timestamp>.csv
         │
         ├── Row: SQL_SERVER / Forecast / incremental / modified_dt
         │   └── Copy: SELECT * FROM dbo.Forecast
         │             WHERE modified_dt > '<last_watermark>'
         │             → landing/o9/weekly/<timestamp>.csv
         │
         ├── Row: SALESFORCE / Forecast__c / full / (none)
         │   └── Copy: SELECT all fields FROM Forecast__c
         │             WHERE Period_Type__c = 'MONTHLY'
         │             → landing/o9/monthly/<timestamp>.csv
         │
         └── Row: SALESFORCE / Forecast__c / full / (none)
             └── Copy: SELECT all fields FROM Forecast__c
                       WHERE Period_Type__c = 'QUARTERLY'
                       → landing/o9/quarterly/<timestamp>.csv
         │
         ▼
  Update watermark in audit.source_extract_metadata (incremental only)
         │
         ▼
  Execute pl_master_etl_pipeline (one call per data_subject)
```

### 2.2 Bronze Flow (01_ingest_to_bronze)

```
Read CSV from landing/o9/<frequency>/
(spark.read.format("csv"), pipe delimiter, header=true, all columns STRING)
         │
         ▼
Add audit columns:
  _file_name        = source filename
  _ingestion_ts     = current timestamp
  _frequency        = daily / weekly / monthly / quarterly
  _batch_id         = UUID from ADF parameter
  _source_batch_nr  = sequence number
  _load_job_nr      = run counter
         │
         ▼
Write to hpe_catalog.bronze.o9_forecast_raw
(Delta, mode=overwrite, replaceWhere on _frequency + _ingestion_date)
         │
         ├── DQ: PK null check (product_id, location_id, forecast_date)
         │   ├── Valid rows   → bronze table
         │   └── Invalid rows → hpe_catalog.bronze.quarantine
         │
         ▼
Archive source CSV → archive/<frequency>/<timestamp>/
         │
         ▼
Write audit entry → audit.pipeline_audit (status=SUCCESS/FAILED)
```

### 2.3 Silver Flow (02_bronze_to_silver)

```
Read hpe_catalog.bronze.o9_forecast_raw
WHERE _batch_id = current batch
         │
         ▼
Null/empty string cleansing (all STRING columns)
         │
         ▼
Type casting:
  forecast_date  → DATE
  forecast_qty   → DECIMAL(12,2)
  revenue_amount → DECIMAL(16,2)
         │
         ▼
PK null filter:
  Valid   → continue
  Invalid → log to audit.data_quality_log
         │
         ▼
Deduplication:
  Window: ROW_NUMBER() OVER (PARTITION BY product_id, location_id,
           forecast_date, _frequency ORDER BY _ingestion_ts DESC)
  Keep:  row_num = 1
  Drop:  row_num > 1
         │
         ▼
Business rule validation:
  forecast_qty   > 0
  currency_code  IN ('USD','EUR','GBP','JPY','INR',...)
  category       IN ('SERVER','STORAGE','COMPUTE','NETWORKING',
                     'PRIVATE_CLOUD','SUPERCOMPUTING','AI')
         │
         ▼
SCD Type 2 MERGE on dimension entities:
  dim_product:  key = product_id + category + sub_category
  dim_customer: key = customer_id + region
  dim_location: key = location_id + country
  (set effective_to, is_active=false on changed records; insert new version)
         │
         ▼
Append to hpe_catalog.silver.o9_forecast_ref
(Delta, mode=append, partitioned by _ingestion_date)
         │
         ▼
Write audit entry → audit.pipeline_audit
```

### 2.4 Gold Flow (03_silver_to_gold)

```
Read hpe_catalog.silver.o9_forecast_ref
WHERE _ingestion_date = current run date
         │
         ▼
Derive period column:
  if apply_concat=true → YYYY-MM-01 (from forecast_date parts)
  else                 → pass through forecast_date
         │
         ▼
Populate dimension tables (SCD Type 2 merge):
  dim_product  → from silver category / sub_category
  dim_location → from silver region / country
  dim_time     → derive quarter, fiscal_year from period
         │
         ▼
Load periodic fact table (hpe_catalog.gold.o9_forecast_dmnsn):
  MERGE on (product_key, location_key, time_key, channel, _frequency)
  INSERT new period records (never overwrite historical periods)
         │
         ▼
Archive processed Silver partitions → archive/silver/<period>/
         │
         ▼
Write audit entry → audit.pipeline_audit
```

### 2.5 KPI Aggregation (04_aggregated_audit)

```
Read hpe_catalog.gold.o9_forecast_dmnsn
WHERE _gold_load_ts = today
         │
         ▼
GROUP BY _file_name, period, category, region
SUM(forecast_qty), SUM(revenue_amount)
         │
         ▼
Transpose (stack):
  (file_name, keyfigure='forecast_qty',   total_qty_amount=SUM(forecast_qty))
  (file_name, keyfigure='revenue_amount', total_qty_amount=SUM(revenue_amount))
         │
         ▼
Append to hpe_catalog.gold.o9_forecast_agg_audit
(source for PowerBI / Tableau dashboards)
```

---

## 3. Database Schemas

### 3.1 Azure SQL — audit-db

```sql
audit.pipeline_metadata        -- one row per data_subject (config)
  data_subject, source_system, source_path, bronze_table,
  silver_table, gold_table, frequency, is_active

audit.source_extract_metadata  -- one row per source object (extraction config)
  source_system, connector, source_schema, source_object,
  landing_path, data_subject, load_type, watermark_column,
  last_watermark, is_active

audit.pipeline_audit           -- one row per notebook run (job tracking)
  batch_id, application_id, object_name, data_layer,
  job_start_ts, job_end_ts, job_status, job_duration_sec,
  source_row_count, target_row_count, error_record_count,
  source_system, file_name, load_job_number

audit.data_quality_log         -- one row per DQ check per run
  batch_id, table_name, check_type, records_checked,
  records_passed, records_failed, check_ts
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
