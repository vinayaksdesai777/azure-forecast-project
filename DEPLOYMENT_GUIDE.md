# Build & Deploy Guide — Azure o9 Forecast Pipeline

A step-by-step runbook to stand up this entire project on Azure from a clean subscription.
Follow the steps in order — each one depends on the previous.

**What you'll end up with:** Source systems (SAP HANA, SQL Server, Salesforce) → ADLS Gen2 landing (5 containers) → Databricks (Bronze/Silver/Gold notebooks) → ADF (orchestration + 4 schedules) → Azure SQL (audit) → Key Vault (secrets).

> **⚠ Read this first if your sources are SAP HANA / SQL Server / Salesforce.**
> The medallion flow in this guide *starts from the `landing` container* — the master pipeline ([pl_master_etl_pipeline.json](adf/pipeline/pl_master_etl_pipeline.json)) only checks `landing/` for files and Bronze reads `abfss://landing@...`. The **source → landing ingestion stage is now implemented** as [pl_extract_to_landing.json](adf/pipeline/pl_extract_to_landing.json) (with its source linked services, datasets, and the `audit.source_extract_metadata` table). Configure and run it (**Step 5.5**) before scheduling.

---

## 0. Prerequisites (one-time, on your machine)

| Tool | Why | Install / check |
|---|---|---|
| Azure subscription | Owner or Contributor + User Access Administrator | `az account show` |
| Azure CLI | Login, resource ops | `az version` |
| Databricks CLI | Upload notebooks, create secret scope | `databricks --version` |
| Git | Repos integration | `git --version` |

Log in and select your subscription:

```powershell
az login
az account set --subscription "<your-subscription-id>"
```

> **Resource names are globally unique.** The names used below (`adlshpeo9dev`, `sql-hpe-o9-dev`, `kv-hpe-o9-dev`) are examples. If any collide, pick your own and keep them consistent across every step.

---

## 1. Provision infrastructure (Azure CLI / Portal)

Create the core resources. Run these in order (or create the equivalents in the Azure Portal).

```powershell
$RG="rg-hpe-data-pipeline"; $LOC="eastus"
az group create -n $RG -l $LOC

# ADLS Gen2 storage account (hierarchical namespace on) + 5 containers
az storage account create -n "adlshpeo9dev" -g $RG -l $LOC `
  --sku Standard_LRS --kind StorageV2 --hns true
foreach ($c in "landing","bronze","silver","gold","archive") {
  az storage fs create -n $c --account-name "adlshpeo9dev"
}

# Azure SQL server + audit database
az sql server create -n "sql-hpe-o9-dev" -g $RG -l $LOC `
  --admin-user "sqladmin" --admin-password "<Strong-Passw0rd!>"
az sql db create -g $RG -s "sql-hpe-o9-dev" -n "audit-db" --service-objective S0

# Key Vault
az keyvault create -n "kv-hpe-o9-dev" -g $RG -l $LOC

# Databricks workspace
az databricks workspace create -n "dbw-hpe-o9-dev" -g $RG -l $LOC --sku standard

# Data Factory (with System Assigned Identity)
az datafactory create -n "adf-hpe-o9-dev" -g $RG -l $LOC
```

**Capture the values** you'll need throughout: ADLS account name, Databricks workspace URL, SQL server FQDN (`sql-hpe-o9-dev.database.windows.net`), Data Factory name.

**✓ Acceptance:** the resource group `rg-hpe-data-pipeline` shows all services in the Azure Portal.

---

## 2. Store all secrets in Key Vault

The notebooks and ADF linked services read every credential from Key Vault — nothing is hard-coded. Set the secrets the code expects:

```powershell
$KV = "kv-hpe-o9-dev"

# ADLS access key (for Databricks + ADF)
$ADLS_KEY = az storage account keys list -n "adlshpeo9dev" -g "rg-hpe-data-pipeline" --query "[0].value" -o tsv
az keyvault secret set --vault-name $KV --name "adls-access-key" --value $ADLS_KEY

# Azure SQL credentials (keys referenced by 00_config.py)
az keyvault secret set --vault-name $KV --name "sql-user" --value "sqladmin"
az keyvault secret set --vault-name $KV --name "sql-password" --value "<Strong-Passw0rd!>"
```

**Secret-name contract** (must match exactly): `sql-user`, `sql-password`, `adls-access-key`. The source extraction linked services (Step 5.5) also read these secrets — set them now if your sources are live:

```powershell
# SAP HANA / SQL Server / Salesforce credentials (referenced by ls_sap_hana / ls_sql_server / ls_salesforce)
az keyvault secret set --vault-name $KV --name "saphana-password"          --value "<sap-hana-password>"
az keyvault secret set --vault-name $KV --name "sqlserver-password"        --value "<sql-server-password>"
az keyvault secret set --vault-name $KV --name "salesforce-password"       --value "<salesforce-password>"
az keyvault secret set --vault-name $KV --name "salesforce-security-token" --value "<salesforce-security-token>"
```

---

## 3. Create the Azure SQL audit schema

Connect to `sql-hpe-o9-dev.database.windows.net` / `audit-db` (SSMS, Azure Data Studio, or `sqlcmd`). First open the firewall to your IP:

```powershell
$MYIP = (Invoke-RestMethod https://api.ipify.org)
az sql server firewall-rule create -g "rg-hpe-data-pipeline" -s "sql-hpe-o9-dev" `
  -n "my-ip" --start-ip-address $MYIP --end-ip-address $MYIP
# Also allow Azure services (Databricks/ADF) to connect:
az sql server firewall-rule create -g "rg-hpe-data-pipeline" -s "sql-hpe-o9-dev" `
  -n "azure-services" --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0
```

Run the SQL scripts in order:

1. [sql/01_audit_tables.sql](sql/01_audit_tables.sql) — creates `audit.pipeline_audit`, `audit.data_quality_log`, `audit.pipeline_metadata` + stored procs (and seeds the 4 metadata rows).
2. [sql/02_aggregation_views.sql](sql/02_aggregation_views.sql) — monitoring views.
3. [sql/03_source_extract_metadata.sql](sql/03_source_extract_metadata.sql) — creates `audit.source_extract_metadata` + watermark proc, seeds per-source extract rows, and sets `pipeline_metadata.source_system` per source (needed for Step 5.5 extraction).

**Seed the metadata** — one row per data subject/frequency. The four ADF triggers pass `o9_daily`, `o9_weekly`, `o9_monthly`, `o9_quarterly`:

```sql
INSERT INTO audit.pipeline_metadata (data_subject, frequency, is_active, ...)
VALUES ('o9_daily','daily',1, ...),
       ('o9_weekly','weekly',1, ...),
       ('o9_monthly','monthly',1, ...),
       ('o9_quarterly','quarterly',1, ...);
```

**✓ Acceptance:** `SELECT * FROM audit.pipeline_metadata` returns 4 active rows.

---

## 4. Set up Databricks (Unity Catalog, secrets, notebooks)

### 4a. Unity Catalog
In the Databricks workspace, create the catalog and schemas the code targets:

```sql
CREATE CATALOG IF NOT EXISTS hpe_catalog;
CREATE SCHEMA IF NOT EXISTS hpe_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS hpe_catalog.silver;
CREATE SCHEMA IF NOT EXISTS hpe_catalog.gold;
```

Then run the data-model DDL: [data_model/01_bronze_schema.sql](data_model/01_bronze_schema.sql), [02_silver_schema.sql](data_model/02_silver_schema.sql), [03_gold_schema.sql](data_model/03_gold_schema.sql).

### 4b. Key Vault-backed secret scope
The notebooks read secrets from a scope named `kv-scope`. Create it backed by your Key Vault:

```powershell
databricks configure --host https://adb-xxxx.azuredatabricks.net   # token auth
databricks secrets create-scope kv-scope `
  --scope-backend-type AZURE_KEYVAULT `
  --resource-id "/subscriptions/<sub>/resourceGroups/rg-hpe-data-pipeline/providers/Microsoft.KeyVault/vaults/kv-hpe-o9-dev" `
  --dns-name "https://kv-hpe-o9-dev.vault.azure.net/"
```

### 4c. Configure storage access
Give the cluster access to ADLS (key-based via the `adls-access-key` secret, or preferably a managed identity). For key-based, set the Spark config on the cluster:

```
fs.azure.account.key.adlshpeo9dev.dfs.core.windows.net {{secrets/kv-scope/adls-access-key}}
```

### 4d. Update `00_config.py` and import notebooks
Edit [databricks/notebooks/00_config.py](databricks/notebooks/00_config.py) for your real values:
- `STORAGE_ACCOUNT = "adlshpeo9dev"` (currently the `your_adls_storage_account` placeholder)
- `AUDIT_JDBC_URL` → `jdbc:sqlserver://sql-hpe-o9-dev.database.windows.net:1433;database=audit-db`

Then connect Databricks **Repos** to your Git repo (recommended), or upload [databricks/notebooks/](databricks/notebooks/) and [databricks/utilities/](databricks/utilities/) directly.

**✓ Acceptance:** running `00_config.py` resolves paths without error; `dbutils.secrets.get(scope="kv-scope", key="sql-user")` returns a value.

---

## 5. Import & configure Azure Data Factory

ADF artifacts live in [adf/](adf/). Import in dependency order: **linked services → datasets → pipelines → triggers**.

### 5a. Grant ADF access to secrets
ADF uses its System Assigned Identity. Give it `Get`/`List` on Key Vault:

```powershell
$ADF_OID = az datafactory show -n "adf-hpe-o9-dev" -g "rg-hpe-data-pipeline" --query "identity.principalId" -o tsv
az keyvault set-policy -n "kv-hpe-o9-dev" --object-id $ADF_OID --secret-permissions get list
```

### 5b. Update linked service references
Edit the JSON to point at your resources before importing:
- [ls_keyvault.json](adf/linkedservices/ls_keyvault.json) → Key Vault URL `https://kv-hpe-o9-dev.vault.azure.net/`
- [ls_adls_gen2.json](adf/linkedservices/ls_adls_gen2.json) → storage account `adlshpeo9dev`
- [ls_azure_databricks.json](adf/linkedservices/ls_azure_databricks.json) → workspace URL + cluster config
- [ls_azure_sql_audit.json](adf/linkedservices/ls_azure_sql_audit.json) → SQL FQDN / `audit-db`

### 5c. Import via CLI (or paste JSON in the ADF Studio UI)

```powershell
$RG="rg-hpe-data-pipeline"; $ADF="adf-hpe-o9-dev"
az datafactory linked-service create --factory-name $ADF -g $RG --name ls_keyvault       --properties "@adf/linkedservices/ls_keyvault.json"
az datafactory linked-service create --factory-name $ADF -g $RG --name ls_adls_gen2      --properties "@adf/linkedservices/ls_adls_gen2.json"
az datafactory linked-service create --factory-name $ADF -g $RG --name ls_azure_databricks --properties "@adf/linkedservices/ls_azure_databricks.json"
az datafactory linked-service create --factory-name $ADF -g $RG --name ls_azure_sql_audit --properties "@adf/linkedservices/ls_azure_sql_audit.json"

# datasets
az datafactory dataset create --factory-name $ADF -g $RG --name ds_landing_csv       --properties "@adf/datasets/ds_landing_csv.json"
az datafactory dataset create --factory-name $ADF -g $RG --name ds_adls_parquet      --properties "@adf/datasets/ds_adls_parquet.json"
az datafactory dataset create --factory-name $ADF -g $RG --name ds_azure_sql_metadata --properties "@adf/datasets/ds_azure_sql_metadata.json"

# pipelines
az datafactory pipeline create --factory-name $ADF -g $RG --name pl_master_etl_pipeline  --pipeline "@adf/pipeline/pl_master_etl_pipeline.json"
```

> The ADF UI (**Manage → ARM/JSON**) is often easier for first-time import since it validates references interactively. CLI is better for repeatable deploys.

**✓ Acceptance:** ADF Studio shows all linked services with green "Test connection" results.

---

## 5.5. Source extraction: SAP HANA / SQL Server / Salesforce → landing

The existing pipeline assumes data already sits in the `landing` container. Your sources are live systems, so you need an **extraction pipeline** that lands each source as files in ADLS, after which the existing Bronze→Silver→Gold flow takes over unchanged.

### 5.5a. Self-hosted Integration Runtime (for on-prem / private sources)

SAP HANA boxes and most SQL Servers sit behind a corporate firewall — ADF's cloud (Azure) IR cannot reach them. Install a **Self-Hosted Integration Runtime (SHIR)** on a VM that has network line-of-sight to those systems:

```powershell
az datafactory integration-runtime self-hosted create `
  --factory-name "adf-hpe-o9-dev" -g "rg-hpe-data-pipeline" --name "shir-onprem"
# Get the auth key, then install the SHIR MSI on the on-prem/VNet VM and register it:
az datafactory integration-runtime list-auth-key `
  --factory-name "adf-hpe-o9-dev" -g "rg-hpe-data-pipeline" --name "shir-onprem"
```

Salesforce is SaaS (public internet), so it uses the **Azure (AutoResolve) IR** — no SHIR needed.

### 5.5b. Source-specific connectors & driver notes

| Source | ADF connector | Auth | Notes |
|---|---|---|---|
| **SAP HANA** | *SAP HANA* (ODBC-based) | user/password (Key Vault) | Needs the **SAP HANA ODBC driver** installed on the SHIR VM. Use a read replica/calc view where possible. For large fact extracts prefer **SAP Table** or **SAP CDC** connector instead. |
| **SQL Server** | *SQL Server* | SQL or Windows auth (Key Vault) | Via SHIR if on-prem; native cloud IR if it's Azure SQL MI with private endpoint. Use a `WHERE` watermark for incremental pulls. |
| **Salesforce** | *Salesforce* (or Salesforce Bulk API 2.0 for volume) | OAuth: client id/secret + security token (Key Vault) | Cloud IR. Query objects with SOQL. Bulk API for >1M rows. |

Add the credentials to Key Vault (Step 2) — the linked services for each source are **already in the repo**, all referencing Key Vault with no inline secrets: [ls_sap_hana.json](adf/linkedservices/ls_sap_hana.json), [ls_sql_server.json](adf/linkedservices/ls_sql_server.json), [ls_salesforce.json](adf/linkedservices/ls_salesforce.json). Edit each for your host/user/environment values, then import (CLI below). The SAP HANA and on-prem SQL Server linked services bind to the SHIR via `connectVia: shir-onprem` ([shir_onprem.json](adf/integrationruntimes/shir_onprem.json)); Salesforce uses the AutoResolve IR.

### 5.5c. Deploy the extraction pipeline (`pl_extract_to_landing`)

The metadata-driven extraction pipeline is **implemented** at [pl_extract_to_landing.json](adf/pipeline/pl_extract_to_landing.json). It runs **before** `pl_master_etl_pipeline` and does:

1. **Lookup** `audit.source_extract_metadata` (one row per active source object) → `source_system`, `connector`, `source_schema`, `source_object`, `watermark_column`, `landing_path`, `load_type`, `last_watermark`, `data_subject`.
2. **ForEach** row → a **Switch** on `connector` runs the matching **Copy activity** (SAP HANA / SQL Server / Salesforce). The source query appends a `WHERE <watermark_column> > <last_watermark>` predicate for `incremental` rows. The sink is [ds_landing_csv.json](adf/datasets/ds_landing_csv.json) — pipe-delimited CSV with header into `landing/<landing_path>/`, exactly what `GetMetadata` and Bronze expect.
3. For **incremental** loads, `audit.usp_update_extract_watermark` pushes the new high-watermark back to SQL after each successful copy.
4. On success it **executes `pl_master_etl_pipeline`** (Execute Pipeline activity) for the parameterized `data_subject`.

Import order — its source datasets first, then the pipeline:

```powershell
# source linked services (after editing host/user values)
az datafactory integration-runtime self-hosted create --factory-name $ADF -g $RG --name shir-onprem
az datafactory linked-service create --factory-name $ADF -g $RG --name ls_sap_hana    --properties "@adf/linkedservices/ls_sap_hana.json"
az datafactory linked-service create --factory-name $ADF -g $RG --name ls_sql_server  --properties "@adf/linkedservices/ls_sql_server.json"
az datafactory linked-service create --factory-name $ADF -g $RG --name ls_salesforce  --properties "@adf/linkedservices/ls_salesforce.json"

# source datasets
az datafactory dataset create --factory-name $ADF -g $RG --name ds_sap_hana_table     --properties "@adf/datasets/ds_sap_hana_table.json"
az datafactory dataset create --factory-name $ADF -g $RG --name ds_sql_server_table   --properties "@adf/datasets/ds_sql_server_table.json"
az datafactory dataset create --factory-name $ADF -g $RG --name ds_salesforce_object  --properties "@adf/datasets/ds_salesforce_object.json"

# extraction pipeline
az datafactory pipeline create --factory-name $ADF -g $RG --name pl_extract_to_landing --pipeline "@adf/pipeline/pl_extract_to_landing.json"
```

> **Contract kept intact:** the pipeline lands **pipe-delimited CSV with header** into the `landing` container under the `landing_path` that `pipeline_metadata.source_path` points to, so the downstream medallion flow needs *zero* changes — the format matches [ds_landing_csv.json](adf/datasets/ds_landing_csv.json) (`columnDelimiter: "|"`, `firstRowAsHeader: true`).

### 5.5d. Create & seed extraction metadata

Run [sql/03_source_extract_metadata.sql](sql/03_source_extract_metadata.sql) against `audit-db`. It creates `audit.source_extract_metadata`, the `audit.usp_update_extract_watermark` proc, seeds one extract row per data subject, and aligns `audit.pipeline_metadata.source_system` to `SAP_HANA` / `SQL_SERVER` / `SALESFORCE` (previously all `o9`) so the audit trail records provenance per layer (see the NO_DATA path at [pl_master_etl_pipeline.json:202](adf/pipeline/pl_master_etl_pipeline.json#L202)). Adjust the seeded `source_schema` / `source_object` / `landing_path` to your real source objects.

**✓ Acceptance:** running `pl_extract_to_landing` drops a pipe-delimited CSV into `landing/<landing_path>/` for each active source; `GetMetadata` in the master pipeline then sees `exists = true` and the medallion flow runs unchanged.

---

## 6. End-to-end smoke test (manual run before scheduling)

1. Land data via the extraction pipeline: **Debug `pl_extract_to_landing`** (Step 5.5) so a real SAP HANA / SQL Server / Salesforce extract drops a pipe-delimited CSV into `landing/o9_forecast_daily/`. *(For an isolated medallion-only test, you can still hand-drop a sample CSV there instead.)*
2. In ADF Studio, **Debug** `pl_master_etl_pipeline` with parameters `data_subject = o9_daily`, `storage_account = adlshpeo9dev`.
3. Watch the run: Lookup (metadata) → GetMetadata (file exists) → IfCondition TRUE → Bronze → Silver → Gold → aggregated audit notebooks.
4. Verify results:

```sql
-- Databricks
SELECT count(*) FROM hpe_catalog.bronze.o9_forecast_raw;
SELECT count(*) FROM hpe_catalog.silver.o9_forecast_ref;
SELECT * FROM hpe_catalog.gold.o9_forecast_dmnsn LIMIT 20;

-- Azure SQL
SELECT TOP 10 * FROM audit.pipeline_audit ORDER BY created_at DESC;
SELECT * FROM vw_daily_load_summary;
```

5. Confirm the source file moved to the `archive` container.

**✓ Acceptance:** Bronze/Silver/Gold all populated; one `SUCCESS` row in `audit.pipeline_audit`; file archived.

---

## 7. Activate schedules

Import and start the four triggers from [adf/triggers/](adf/triggers/):

| Trigger | Schedule (UTC) | `data_subject` |
|---|---|---|
| [tr_daily_schedule.json](adf/triggers/tr_daily_schedule.json) | 06:00 daily | o9_daily |
| [tr_weekly_schedule.json](adf/triggers/tr_weekly_schedule.json) | Mon 07:00 | o9_weekly |
| [tr_monthly_schedule.json](adf/triggers/tr_monthly_schedule.json) | 1st 08:00 | o9_monthly |
| [tr_quarterly_schedule.json](adf/triggers/tr_quarterly_schedule.json) | Quarter start 09:00 | o9_quarterly |

```powershell
az datafactory trigger create --factory-name $ADF -g $RG --name tr_daily_schedule     --properties "@adf/triggers/tr_daily_schedule.json"
az datafactory trigger start  --factory-name $ADF -g $RG --name tr_daily_schedule
# repeat for weekly, monthly, quarterly
```

**✓ Acceptance:** all four triggers show **Started** in ADF Studio → Manage → Triggers.

---

## 8. Validate before going live

```powershell
# Run the unit tests locally
pip install pyspark pytest
pytest tests/ -v
```

Then confirm governance/observability:
- Unity Catalog lineage shows Bronze → Silver → Gold.
- `vw_dq_summary` reflects pass rates; seed a bad row and confirm it drops below 100%.
- A read-only analyst can `SELECT` Gold but not Bronze/Silver (apply UC grants).

---

## Deployment order at a glance

```
Provision infra (CLI/Portal)  →  Key Vault secrets  →  SQL audit schema + seed
      →  Databricks (UC, secret scope, storage, notebooks)  →  ADF (LS → datasets → pipelines → triggers)
      →  Source extraction (SHIR + connectors → landing)  →  Manual smoke test  →  Start triggers  →  Tests + governance checks
```

## Common gotchas

- **Placeholder values still in code:** `00_config.py` ships with `your_adls_storage_account` and `your-sql-server` — these *must* be updated (Step 4d).
- **Secret scope name:** notebooks expect `kv-scope`; the scope name is case-sensitive.
- **SQL firewall:** add both your IP *and* "Allow Azure services" or Databricks/ADF can't reach the audit DB.
- **ADF identity on Key Vault:** without the access policy in Step 5a, every linked service that resolves a secret will fail.
- **SAP HANA ODBC driver:** must be installed on the SHIR VM, or the `ls_sap_hana` test connection fails.
- **Globally-unique names:** if a storage/SQL/Key Vault name is taken, pick your own and keep it consistent across every step.
</content>
</invoke>
