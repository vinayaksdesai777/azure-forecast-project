# Technical Specification
## HPE o9 Forecast Data Pipeline — Azure Medallion Architecture

**Version:** 1.0  
**Date:** 2026-06-30  
**Author:** Vinayak S Desai

---

## 1. Purpose

This document specifies the architecture, technology choices, and design decisions for the HPE o9 Forecast Data Pipeline. The pipeline ingests forecast data from three heterogeneous source systems into Azure, processes it through a medallion lakehouse (Bronze → Silver → Gold), and surfaces KPIs for reporting.

---

## 2. Scope

| In Scope | Out of Scope |
|---|---|
| Data ingestion from SAP HANA, SQL Server, Salesforce | Source system ERP/CRM logic |
| Landing → Bronze → Silver → Gold transformation | PowerBI / Tableau report design |
| Audit logging and data quality tracking | User authentication / SSO |
| ADF orchestration and scheduling | Salesforce CRM business workflows |
| Unity Catalog governance on Databricks | Network / VPN infrastructure |

---

## 3. Architecture Overview

```
SOURCE SYSTEMS
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  SAP HANA    │  │  SQL Server  │  │  Salesforce  │
│  Cloud       │  │  On-Prem     │  │  Dev Edition │
│  (Daily)     │  │  (Weekly)    │  │  (Monthly +  │
│  9,800 rows  │  │  9,800 rows  │  │   Quarterly) │
│              │  │              │  │  9,800 × 2   │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                 │
       └─────────────────┴─────────────────┘
                         │
                   ADF: pl_extract_to_landing
                   (watermark-driven incremental
                    for HANA + SQL Server;
                    full load for Salesforce)
                         │
                         ▼
              ┌─────────────────────┐
              │   ADLS Gen2         │
              │   landing/          │
              │   ├── o9/daily/     │
              │   ├── o9/weekly/    │
              │   ├── o9/monthly/   │
              │   └── o9/quarterly/ │
              │   (pipe-delimited   │
              │    CSV files)       │
              └──────────┬──────────┘
                         │
                   ADF: pl_master_etl_pipeline
                   (parameterized by data_subject)
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
       Bronze          Silver          Gold
  hpe_catalog.bronze  hpe_catalog.silver  hpe_catalog.gold
  o9_forecast_raw     o9_forecast_ref     o9_forecast_dmnsn
  (Delta, all STRING) (Delta, typed)      (Delta, star schema)
          │
          ▼
   Quarantine table
   bronze.quarantine
   (bad PK rows)
                         │
                         ▼
              ┌──────────────────────────────┐
              │  Databricks Unity Catalog    │
              │  hpe_catalog.audit           │
              │  ├── job_log (Delta)         │
              │  │   batch_id, insert_time,  │
              │  │   records_inserted,       │
              │  │   records_updated,        │
              │  │   status, layer           │
              │  └── data_quality_log(Delta) │
              │                              │
              │  Azure SQL (pipeline config) │
              │  audit.pipeline_metadata     │
              │  audit.source_extract_       │
              │        metadata              │
              └──────────────────────────────┘
```

---

## 4. Technology Stack

| Component | Technology | Version / Tier | Purpose |
|---|---|---|---|
| Orchestration | Azure Data Factory | v2 | Pipeline scheduling, metadata-driven execution |
| Storage | Azure Data Lake Storage Gen2 | Standard LRS | Landing, Bronze, Silver, Gold, Archive containers |
| Compute | Azure Databricks | Standard / Unity Catalog | Notebook-based medallion transformations |
| Audit DB | Azure SQL Database | S0 tier | Pipeline audit, DQ logs, metadata config |
| Secrets | Azure Key Vault | Standard | All credentials, never hard-coded |
| IaC | Terraform | Latest | Infrastructure provisioning |
| Source 1 | SAP HANA Cloud | Free Tier (BTP Trial, cf-ap21) | Daily forecast — incremental |
| Source 2 | SQL Server | Express (on-prem, SSMS v19) | Weekly forecast — incremental |
| Source 3 | Salesforce | Developer Edition | Monthly + quarterly forecast — full load |
| SHIR | Self-Hosted Integration Runtime | Latest | On-prem SQL Server connectivity to ADF |
| Governance | Unity Catalog | Databricks UC | Table-level and column-level access control |

---

## 5. Source Systems

### 5.1 SAP HANA Cloud
| Property | Value |
|---|---|
| Host | `<instance>.hanacloud.ondemand.com` |
| Port | 443 (SSL) |
| Schema | `O9_SOURCE` |
| Object | `FORECAST_VIEW` (column table) |
| Load type | Incremental |
| Watermark column | `CHANGED_ON` |
| ADF connector | SAP HANA (ODBC) |
| Integration runtime | AutoResolve (public internet) |
| Rows | 9,800 (9,000 clean + 800 dirty) |

**Dirty data injected:**
- 300 rows with NULL `CUSTOMER_ID`
- 200 rows with negative `FORECAST_QTY`
- 200 duplicate rows (same `CHANGED_ON`)
- 100 rows with invalid `CATEGORY = 'LEGACY_HW'`

### 5.2 SQL Server (On-Premises)
| Property | Value |
|---|---|
| Server | `DESKTOP-TN8JRN7\SQLEXPRESS` |
| Database | `HPE_SOURCE` |
| Schema | `dbo` |
| Table | `Forecast` |
| Load type | Incremental |
| Watermark column | `modified_dt` |
| ADF connector | SQL Server |
| Integration runtime | `shir-onprem` (Self-Hosted IR) |
| Rows | 9,800 (9,000 clean + 800 dirty) |

**Dirty data injected:**
- 300 rows with mixed-case `category` (e.g. `'server'`, `'Storage'`)
- 200 rows with NULL `revenue_amount`
- 200 duplicate rows (same `modified_dt`)
- 100 rows with invalid `currency = 'XX'`

**Note:** SQL Server default collation (`Latin1_General_CI_AS`) is case-insensitive. Use `COLLATE Latin1_General_CS_AS` for case-sensitive DQ checks in Silver.

### 5.3 Salesforce
| Property | Value |
|---|---|
| Environment | Developer Edition (`login.salesforce.com`) |
| Object | `Forecast__c` (custom object) |
| Load type | Full (no watermark) |
| ADF connector | Salesforce (Bulk API 2.0) |
| Integration runtime | AutoResolve |
| Rows | 9,800 × 2 (monthly + quarterly) |

**Dirty data injected (both files):**
- 300 rows with NULL `Customer_Id__c`
- 200 rows with negative `Forecast_Qty__c`
- 200 duplicate rows (same fiscal period + product)
- 100 rows with invalid `Currency_Code__c = 'XX'`

---

## 6. Data Model

### 6.1 Common Source Schema (all sources produce these columns)

| Column | Type | Description |
|---|---|---|
| `product_id` | STRING | HPE product identifier |
| `location_id` | STRING | Ship-to / planning location |
| `forecast_date` | STRING → DATE | Planning horizon date |
| `forecast_qty` | STRING → DECIMAL | Units forecasted |
| `revenue_amount` | STRING → DECIMAL | Revenue in local currency |
| `customer_id` | STRING | Customer identifier |
| `channel` | STRING | DIRECT / PARTNER / DISTRIBUTOR / ONLINE |
| `category` | STRING | SERVER / STORAGE / COMPUTE / NETWORKING / PRIVATE_CLOUD / SUPERCOMPUTING / AI |
| `sub_category` | STRING | Product sub-group |
| `region` | STRING | NORTH_AMERICA / EUROPE / ASIA_PACIFIC / LATIN_AMERICA / MIDDLE_EAST |
| `country` | STRING | ISO 2-letter country code |
| `currency` | STRING | ISO 4217 currency code |
| `uom` | STRING | Unit of measure (UNIT) |

### 6.2 Medallion Layer Contracts

**Landing** — Raw CSV, pipe-delimited, with timestamp in filename.

**Bronze** (`hpe_catalog.bronze.o9_forecast_raw`) — All columns as STRING (schema-on-read). Adds:
- `_file_name`, `_ingestion_ts`, `_frequency`, `_batch_id`, `_source_batch_nr`, `_load_job_nr`

**Silver** (`hpe_catalog.silver.o9_forecast_ref`) — Typed columns (DATE, DECIMAL). Adds:
- `_ingestion_date`, `_silver_load_ts`, `_batch_id`
- NOT NULL constraints on `product_id`, `location_id`, `forecast_date`
- SCD Type 2 columns on dimension entities: `effective_from`, `effective_to`, `is_active`

**Gold** (`hpe_catalog.gold.o9_forecast_dmnsn`) — Partitioned by `(period, _frequency)`. Star schema:
- Fact: `o9_forecast_dmnsn` — periodic fact, append-only by period
- Dims: `dim_product`, `dim_location`, `dim_time` — SCD Type 2
- KPI: `o9_forecast_agg_audit` — keyfigure/amount aggregations for PowerBI/Tableau

**Audit / Job Tracking** (`hpe_catalog.audit`) — Delta tables in Unity Catalog (not Azure SQL):
- `job_log` — one row per notebook run: `batch_id`, `insert_time`, `records_inserted`, `records_updated`, `status`, `layer`
- `data_quality_log` — one row per DQ check per run

**Azure SQL** (`audit-db`) — used only for pipeline configuration, not job tracking:
- `audit.pipeline_metadata` — runtime config per data_subject (paths, table names, frequency)
- `audit.source_extract_metadata` — extraction config per source object (watermarks, connectors)

### 6.3 Star Schema

```
                    ┌─────────────┐
                    │  dim_time   │
                    │  time_key   │
                    │  period     │
                    │  fiscal_yr  │
                    │  quarter    │
                    │  month      │
                    └──────┬──────┘
                           │
┌─────────────┐    ┌───────┴──────────┐    ┌──────────────┐
│ dim_product │────│fact_o9_forecast  │────│ dim_location │
│ product_key │    │ product_key (FK) │    │ location_key │
│ product_id  │    │ location_key(FK) │    │ location_id  │
│ category    │    │ time_key (FK)    │    │ region       │
│ sub_cat     │    │ customer_id      │    │ country      │
│ effective_  │    │ channel          │    │ effective_   │
│  from/to    │    │ forecast_qty     │    │  from/to     │
│ is_active   │    │ revenue_amount   │    │ is_active    │
└─────────────┘    │ currency         │    └──────────────┘
                   │ uom              │
                   │ period           │
                   │ _frequency       │
                   └──────────────────┘
```

---

## 7. Design Decisions

### 7.1 Schema-on-read in Bronze
All source columns land as STRING in Bronze. Type casting happens in Silver. This decouples ingestion from schema changes — if a source adds a column or changes a type, Bronze absorbs it without breaking.

### 7.2 Incremental vs Full Load
- SAP HANA and SQL Server use **incremental** loads driven by watermark columns (`CHANGED_ON`, `modified_dt`). The last watermark is stored in `audit.source_extract_metadata` and updated after each successful extract.
- Salesforce uses **full load** because the Developer Edition API does not expose a reliable system-modified timestamp across custom objects at scale.

### 7.3 Metadata-driven Orchestration
A single ADF pipeline (`pl_extract_to_landing`) reads `audit.source_extract_metadata` at runtime — adding a new source only requires inserting a row into that table, no pipeline changes needed. Similarly, `pl_master_etl_pipeline` reads `audit.pipeline_metadata` to determine paths and parameters.

### 7.4 Unity Catalog for Governance
All Delta tables are created under `hpe_catalog` with three-part naming (`hpe_catalog.layer.table`). Unity Catalog enforces row/column-level access and provides a unified lineage view across all Databricks notebooks.

### 7.5 Self-Hosted IR for On-Prem SQL Server
SQL Server runs on-premises (`SQLEXPRESS`). ADF cannot reach it over the public internet. A Self-Hosted Integration Runtime (`shir-onprem`) installed on the same machine bridges the connection.

### 7.6 Dirty Data by Design
All three sources contain intentional dirty data (NULLs, negatives, duplicates, invalid categories/currencies). This reflects real-world data engineering — Silver is the cleansing layer, not the sources.

### 7.7 Quarantine Table
Rows that fail Bronze DQ checks (NULL primary keys, unresolvable type issues) are routed to `bronze.quarantine` rather than being silently dropped or blocking the pipeline. This allows investigation and reprocessing.

---

## 8. Security

| Secret | Key Vault Name | Used By |
|---|---|---|
| SAP HANA password | `saphana-password` | `ls_sap_hana` linked service |
| SQL Server password | `sqlserver-password` | `ls_sql_server` linked service |
| Salesforce password | `salesforce-password` | `ls_salesforce` linked service |
| Salesforce security token | `salesforce-security-token` | `ls_salesforce` linked service |
| ADLS access key | `adls-access-key` | Databricks + `ls_adls_gen2` |
| Azure SQL user | `sql-user` | Databricks JDBC |
| Azure SQL password | `sql-password` | Databricks JDBC |

No credentials are hard-coded anywhere in notebooks, pipelines, or linked service JSON files.

---

## 9. Implementation Gaps & Target Design

This section documents what the current code does vs what it must do. All items below are **required implementation changes** — not optional enhancements.

---

### 9.1 Landing Zone

| Aspect | Current (Code) | Target (Plan) |
|---|---|---|
| File format | CSV (pipe-delimited) | **Parquet** with timestamp in filename |
| ADF sink dataset | `ds_landing_csv` (DelimitedText) | `ds_adls_parquet` (Parquet) |
| Salesforce row limit | `LIMIT 200` hard-coded in SOQL | Remove limit — full extract |

**Changes required:**
- ADF: Switch all 3 Copy activity sinks from `ds_landing_csv` → `ds_adls_parquet`
- ADF: Remove `LIMIT 200` from Salesforce SOQL query
- Bronze notebook: Change `spark.read.format("csv")` → `spark.read.format("parquet")`
- Bronze notebook: Update `_source_batch_nr` regex (currently matches `_(\d{12})\.csv$`)

---

### 9.2 Bronze Zone

| Aspect | Current (Code) | Target (Plan) |
|---|---|---|
| `insert_time` per row | `_ingestion_ts = current_timestamp()` ✅ | ✅ Already correct |
| Column standardization | None — raw copy only | UPPER(category), TRIM all strings, align column names across sources |
| PK null check | In Silver notebook (wrong layer) | Move to Bronze — route bad rows to quarantine |
| Deduplication | Not done | Exact duplicate drop in Bronze before writing Delta |
| Quarantine table | Does not exist | `hpe_catalog.bronze.quarantine` — receives all DQ-failed rows |
| Success/failure status | Writes to Azure SQL `pipeline_audit` | Write to `hpe_catalog.audit.job_log` (Delta, Unity Catalog) |
| Write mode | `overwrite` | `overwrite` with `replaceWhere` on `_frequency + _ingestion_date` |

**Bronze target flow:**
```
Read Parquet from landing/
    │
    ├── Add audit cols (_ingestion_ts, _batch_id, _frequency, _file_name ...)
    ├── TRIM all string columns
    ├── UPPER(category)
    │
    ├── PK null check (product_id, location_id, forecast_date)
    │   ├── Valid   → deduplicate → write to bronze.o9_forecast_raw (Delta)
    │   └── Invalid → write to bronze.quarantine (Delta)
    │
    └── Write status to hpe_catalog.audit.job_log
```

**DDL addition required — `01_bronze_schema.sql`:**
```sql
CREATE TABLE IF NOT EXISTS hpe_catalog.bronze.quarantine (
    -- all source columns as STRING
    product_id      STRING, location_id STRING, forecast_date STRING,
    forecast_qty    STRING, revenue_amount STRING, customer_id STRING,
    channel STRING, category STRING, sub_category STRING,
    region STRING, country STRING, currency STRING, uom STRING,
    -- audit cols
    _file_name      STRING,
    _ingestion_ts   TIMESTAMP,
    _batch_id       STRING,
    _frequency      STRING,
    _dq_fail_reason STRING  COMMENT 'Why this row was quarantined'
)
USING DELTA
COMMENT 'Bronze quarantine: rows that failed PK null check or type validation';
```

---

### 9.3 Silver Zone

| Aspect | Current (Code) | Target (Plan) |
|---|---|---|
| SCD Type 2 | Not implemented — simple `append` write | Delta MERGE on product, customer, location, forecast with `effective_from`, `effective_to`, `is_active` |
| Aggregations | None | Period-level aggregations written to silver agg table |
| Business logic | Empty string → NULL only | Currency allowlist validation, category allowlist validation, case standardization |
| Deduplication | Not in Silver | Moved to Bronze (see 9.2) |
| Schema — SCD2 cols | Missing from `02_silver_schema.sql` | Add `effective_from DATE`, `effective_to DATE`, `is_active BOOLEAN` |

**SCD Type 2 logic (target):**
```
For each incoming Silver row:
  MATCH on (product_id, location_id, forecast_date, _frequency)
  IF matched AND values changed:
    UPDATE existing row: set effective_to = today, is_active = false
    INSERT new row:      set effective_from = today, effective_to = NULL, is_active = true
  IF matched AND no change:
    skip (idempotent)
  IF no match:
    INSERT new row: effective_from = today, effective_to = NULL, is_active = true
```

**Business logic rules:**
```python
VALID_CATEGORIES = ['SERVER','STORAGE','COMPUTE','NETWORKING',
                    'PRIVATE_CLOUD','SUPERCOMPUTING','AI']
VALID_CURRENCIES = ['USD','EUR','GBP','JPY','INR','SGD','AED','BRL']

# Rows failing these → flagged in audit.data_quality_log, not quarantined
# (NULL category/currency is allowed; WRONG value is flagged)
```

**Schema additions required — `02_silver_schema.sql`:**
```sql
effective_from   DATE     COMMENT 'SCD2: record valid from this date',
effective_to     DATE     COMMENT 'SCD2: record valid until this date (NULL = current)',
is_active        BOOLEAN  COMMENT 'SCD2: true = current version of the record'
```

---

### 9.4 Gold Zone

| Aspect | Current (Code) | Target (Plan) |
|---|---|---|
| Write mode | `mode("overwrite")` — destroys all history | MERGE — append new periods, never overwrite past periods |
| Periodic fact table | Single table `o9_forecast_dmnsn` used as both fact + dim | Separate `fact_o9_forecast` + `dim_product` / `dim_location` / `dim_time` |
| Dim table population | DDL exists, **no notebook populates them** | Notebook 03 must MERGE-populate all 3 dim tables |
| Archive tables | Does not exist | Archive processed Silver partitions after Gold load |
| KPI filter | `_ingestion_ts = today` — breaks on reruns | Filter by `_batch_id` instead |
| Dead code bug | Lines 65–76 in `03_silver_to_gold.py`: dangling `.partitionBy("period")` chain never calls `.saveAsTable()` — table written twice | Remove lines 65–76 entirely |

**Gold target flow:**
```
Read Silver (is_active = true, current batch)
    │
    ├── Populate dim_product  (SCD2 MERGE on product_id + category + sub_category)
    ├── Populate dim_location (SCD2 MERGE on location_id + country + region)
    ├── Populate dim_time     (INSERT missing date_keys for all forecast_dates)
    │
    ├── Build fact rows (join silver → dim keys)
    │
    ├── MERGE into fact_o9_forecast
    │   MATCH on (product_key, location_key, time_key, channel, _frequency)
    │   INSERT new — never UPDATE past periods
    │
    ├── Archive Silver partition → archive/silver/<period>/
    │
    └── Write status to hpe_catalog.audit.job_log
```

**DDL additions required — `03_gold_schema.sql`:**
```sql
-- Rename existing o9_forecast_dmnsn → fact_o9_forecast
-- Add surrogate keys to dim tables
-- Add archive table
CREATE TABLE IF NOT EXISTS hpe_catalog.gold.archive_o9_forecast (
    -- same schema as fact_o9_forecast
    -- partitioned by (period, _frequency)
    _archived_ts  TIMESTAMP COMMENT 'When this partition was archived'
)
USING DELTA
COMMENT 'Gold archive: historical fact partitions moved after retention period';
```

---

### 9.5 Audit / Job Tracking

| Aspect | Current (Code) | Target (Plan) |
|---|---|---|
| Store | Azure SQL `audit.pipeline_audit` | `hpe_catalog.audit.job_log` (Delta, Unity Catalog) |
| Columns | `target_row_count` (no insert/update split) | `records_inserted` + `records_updated` separately |
| DQ log store | Azure SQL `audit.data_quality_log` | `hpe_catalog.audit.data_quality_log` (Delta) |

**Target schema — `hpe_catalog.audit.job_log`:**
```sql
CREATE TABLE IF NOT EXISTS hpe_catalog.audit.job_log (
    batch_id          STRING    NOT NULL,
    insert_time       TIMESTAMP NOT NULL,
    layer             STRING    COMMENT 'bronze / silver / gold / agg_audit',
    status            STRING    COMMENT 'SUCCESS / FAILED',
    records_inserted  BIGINT,
    records_updated   BIGINT,
    source_system     STRING,
    data_subject      STRING,
    error_message     STRING
)
USING DELTA
COMMENT 'Unified job tracking log — replaces Azure SQL pipeline_audit';
```

---

### 9.6 Missing `00_config` Notebook

Every notebook calls `%run ./00_config` but this file **does not exist** in the repo. This is a hard blocker — nothing runs without it.

**Must contain:**
```python
# Catalog and schema references
CATALOG         = "hpe_catalog"
BRONZE_SCHEMA   = f"{CATALOG}.bronze"
SILVER_SCHEMA   = f"{CATALOG}.silver"
GOLD_SCHEMA     = f"{CATALOG}.gold"
AUDIT_SCHEMA    = f"{CATALOG}.audit"

# ADLS paths
CONTAINER_LANDING = "landing"
CONTAINER_ARCHIVE = "archive"

# Helper functions
def get_batch_id(prefix): ...       # UUID-based batch ID
def get_timestamp(fmt): ...         # formatted timestamp
def get_pipeline_metadata(data_subject): ...  # reads audit.pipeline_metadata
def get_adls_path(container, path): ...       # builds abfss:// path

# Spark config
spark.sql(f"USE CATALOG {CATALOG}")
```

---

### 9.7 Implementation Priority Order

```
1. 00_config notebook          ← blocker for everything
2. Landing → Parquet           ← ADF sink change + Bronze read change
3. Bronze redesign             ← standardize + PK check + dedup + quarantine
4. Audit → Unity Catalog       ← job_log Delta table replaces Azure SQL
5. Silver → SCD Type 2         ← Delta MERGE + schema additions
6. Gold → periodic fact        ← MERGE write + dim population + archive
7. Fix Salesforce LIMIT 200    ← ADF pipeline fix
8. Fix Gold dead code bug      ← remove lines 65-76 in notebook 03
```
