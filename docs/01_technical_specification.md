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

## 9. Planned Enhancements (Phase 2)

| Item | Description |
|---|---|
| Parameterized ADF trigger | Single trigger with `frequency` parameter replacing 4 separate triggers |
| SCD Type 2 in Silver | Delta MERGE-based history tracking for product, customer, location, forecast dimensions |
| Periodic fact table | Append-only Gold fact table; no overwrite of historical periods |
| Bronze quarantine | Route DQ failures to `bronze.quarantine` instead of dropping |
| Job tracking Delta table | Replace `audit.pipeline_audit` (Azure SQL) with `hpe_catalog.audit.job_log` (Delta, Unity Catalog). Columns: `batch_id`, `insert_time`, `records_inserted`, `records_updated`, `status`, `layer`. Governed by UC access control alongside all other tables. |
| `00_config` notebook | Create missing shared config notebook (catalog name, storage paths, batch ID generation) |
