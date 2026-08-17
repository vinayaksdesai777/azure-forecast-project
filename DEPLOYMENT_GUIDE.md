# Build & Deploy Guide — Azure o9 Forecast Pipeline

A runbook to stand this project up on Azure from a clean subscription. Steps are ordered;
each depends on the one before.

**What you end up with:** SAP HANA Cloud / on-prem SQL Server / Salesforce → ADLS Gen2
landing (Parquet) → Databricks (`landing → silver → gold`) → ADF (two pipelines, four
schedules) → Unity Catalog Delta for config and audit.

> **No Azure SQL, no Key Vault, no Terraform.** Earlier revisions used all three. Audit and
> config now live in Unity Catalog Delta tables, secrets in a Databricks secret scope, and
> deployment is CLI + REST. There is also **no Bronze layer** — landing Parquet plus the
> archive container is the raw archive. See [docs/01_technical_specification.md](docs/01_technical_specification.md) §3.1.

---

## 0. Prerequisites

| Tool | Why | Check |
|---|---|---|
| Azure subscription | Owner or Contributor + User Access Administrator | `az account show` |
| Azure CLI | Resource provisioning | `az version` |
| Databricks CLI | Secret scope, notebook import | `databricks --version` |
| Git | Databricks Repos integration | `git --version` |

```powershell
az login
az account set --subscription "<your-subscription-id>"
```

> **Resource names are globally unique.** `adlshpeo9dev`, `dbw-hpe-o9-dev`, and
> `adf-hpe-o9-dev` below are examples. If one collides, pick your own and keep it
> consistent across every step.

---

## 1. Provision infrastructure

```powershell
$RG="rg-hpe-data-pipeline"; $LOC="eastus"
az group create -n $RG -l $LOC

# ADLS Gen2 (hierarchical namespace on) + containers
az storage account create -n "adlshpeo9dev" -g $RG -l $LOC `
  --sku Standard_LRS --kind StorageV2 --hns true
foreach ($c in "landing","silver","gold","archive","unity-catalog") {
  az storage fs create -n $c --account-name "adlshpeo9dev"
}

# Databricks workspace (Premium — Unity Catalog requires it)
az databricks workspace create -n "dbw-hpe-o9-dev" -g $RG -l $LOC --sku premium

# Data Factory with a System Assigned Identity
az datafactory create -n "adf-hpe-o9-dev" -g $RG -l $LOC
```

Capture the ADLS account name, the Databricks workspace URL, and the Data Factory name.

> There is no `bronze` container. If you are redeploying over an older environment that
> has one, leave it — `sql/13_drop_bronze_layer.sql` cleans up the catalog side.

**✓** All resources visible in `rg-hpe-data-pipeline`.

---

## 2. Unity Catalog

Create an Access Connector for Databricks and grant it storage access:

```powershell
az databricks access-connector create -n "ac-hpe-o9-dev" -g $RG -l $LOC `
  --identity-type SystemAssigned

$AC_ID = az databricks access-connector show -n "ac-hpe-o9-dev" -g $RG `
  --query "identity.principalId" -o tsv
$SA_ID = az storage account show -n "adlshpeo9dev" -g $RG --query id -o tsv

az role assignment create --assignee $AC_ID `
  --role "Storage Blob Data Contributor" --scope $SA_ID
```

Then in a Databricks SQL editor or notebook:

```sql
-- Storage credential + external location (Catalog Explorer can also do this)
CREATE CATALOG IF NOT EXISTS hpe_catalog
  MANAGED LOCATION 'abfss://unity-catalog@adlshpeo9dev.dfs.core.windows.net/';
```

Run the data-model DDL **in order**:

1. [data_model/02_silver_schema.sql](data_model/02_silver_schema.sql) — Silver tables + quarantine
2. [data_model/03_gold_schema.sql](data_model/03_gold_schema.sql) — dimensions, fact, `v_forecast_star`
3. [data_model/04_audit_schema.sql](data_model/04_audit_schema.sql) — `job_log`, `data_quality_log`
4. [data_model/05_pipeline_config.sql](data_model/05_pipeline_config.sql) — `pipeline_config` + the four seed rows

> [data_model/01_bronze_schema.sql](data_model/01_bronze_schema.sql) is retained for
> history only. **Do not run it** on a new deployment.

**✓** `SELECT * FROM hpe_catalog.audit.pipeline_config WHERE is_active = true` returns 4 rows.

---

## 3. Databricks secrets

All credentials live in the scope **`hpe-forecast`**. Nothing is read from Key Vault.

```powershell
databricks configure --host https://adb-xxxx.azuredatabricks.net
databricks secrets create-scope hpe-forecast
```

[databricks/secrets_setup.sh](databricks/secrets_setup.sh) populates the scope. The key
contract the code expects:

| Key | Consumer |
|---|---|
| `adls-account-name`, `adls-account-key` | `00_config.py` |
| `hana-host`, `hana-user`, `hana-password` | `01_ingest_hana_to_landing` |
| `sqlserver-user`, `sqlserver-password` | `ls_sql_server` |
| `salesforce-user`, `salesforce-password`, `salesforce-token` | `ls_salesforce` |
| `sp-client-id`, `sp-client-secret`, `sp-tenant-id` | ADLS OAuth service principal |

**✓** `dbutils.secrets.get(scope="hpe-forecast", key="adls-account-name")` returns a value.

---

## 4. Cluster and notebooks

### 4a. Cluster

Create a cluster with **Single User** access mode. This is not optional: the SAP HANA
JDBC driver has to be installed as a cluster JAR, and Unity Catalog's artifact allowlist
blocks JAR installs on Shared clusters unless a metastore admin adds an allowlist entry.

Upload `ngdbc.jar` (from the SAP HANA client) to the cluster libraries.

### 4b. Notebooks

Connect Databricks **Repos** to this Git repo. `00_config.py` derives its own path at
runtime, so it works from any Repos or Workspace layout without editing.

No placeholder edits are needed — `STORAGE_ACCOUNT` resolves from the secret scope.

**✓** Running `00_config.py` prints the catalog and storage account without error.

---

## 5. Seed the source systems

| Source | Script | Notes |
|---|---|---|
| SAP HANA — daily | [sql/05_hana_daily_data.sql](sql/05_hana_daily_data.sql) | ~547,800 rows |
| SAP HANA — quarterly | [sql/04_hana_quarterly_data.sql](sql/04_hana_quarterly_data.sql) | 600,000 rows |
| SQL Server — weekly | [sql/06_sqlserver_weekly_data.sql](sql/06_sqlserver_weekly_data.sql) | ~390,000 rows; starts with `USE HPE_SOURCE` |
| Salesforce — monthly | [salesforce/generate_full_load.py](salesforce/generate_full_load.py) | ~390,000 generated |

Delta scripts for incremental runs: [07](sql/07_hana_daily_delta.sql),
[08](sql/08_hana_quarterly_delta.sql), [09](sql/09_sqlserver_weekly_delta.sql), and
`salesforce/generate_monthly_delta.py`.

> **Business keys are derived from position within the period**, not from the global row
> number: product from `(period_offset * blocks + (i - 1) / 200) % 500`, location from
> `(i - 1) % 200`. This gives every period a full set of distinct product-location pairs.
>
> The earlier convention keyed product to `(row % 500)` and location to `(row * 7) % 200`.
> Those two cycles realign every `lcm(500, 200) = 1000` rows, so any period with more than
> 1,000 rows collapsed onto just 1,000 real combinations — 30,000 rows/quarter became 1,000
> pairs repeated 30 times. Only SQL Server caught it (its unique constraint threw); HANA and
> Salesforce have no such constraint and loaded the duplicates silently.
>
> **Dimension attributes are keyed to those same indexes**, not to the row number.
> `category` and `sub_category` derive from the product key, `region`, `country`, and
> `currency` from the location index. If you edit these scripts, preserve both properties.
> Keying an attribute to the row makes every product cycle through all seven categories, and
> Gold then versions the dimension member on every load — `dim_product` grew to 1,354 rows
> for 500 products and the star view fanned out to nearly double the fact count.
>
> `05_hana_daily_data.sql` still keys off the row number and is intentionally unchanged:
> at 300 rows/day it never reaches the 1,000-row cycle, so it produces no duplicates.

> **Salesforce Developer Edition caps at 10 MB**, so roughly 84,000 of 108,000 records
> load. Expected; the audit counts reflect what actually arrived.

**✓** Each source table returns its expected row count.

---

## 6. Deploy Azure Data Factory

Artifacts live in [adf/](adf/). Import in dependency order: linked services → datasets →
integration runtime → pipelines → triggers.

### 6a. Self-hosted IR (on-prem SQL Server)

```powershell
$RG="rg-hpe-data-pipeline"; $ADF="adf-hpe-o9-dev"
az datafactory integration-runtime self-hosted create `
  --factory-name $ADF -g $RG --name "shir-hpe-forecast"
az datafactory integration-runtime list-auth-key `
  --factory-name $ADF -g $RG --name "shir-hpe-forecast"
```

Install the SHIR MSI on the machine with SQL Server line-of-sight and register it with
the auth key. Salesforce and ADLS use the AutoResolve IR; SAP HANA does not use ADF at all.

### 6b. Import

```powershell
# linked services
az datafactory linked-service create --factory-name $ADF -g $RG --name ls_adls_gen2        --properties "@adf/linkedService/ls_adls_gen2.json"
az datafactory linked-service create --factory-name $ADF -g $RG --name ls_azure_databricks --properties "@adf/linkedService/ls_azure_databricks.json"
az datafactory linked-service create --factory-name $ADF -g $RG --name ls_sql_server       --properties "@adf/linkedService/ls_sql_server.json"
az datafactory linked-service create --factory-name $ADF -g $RG --name ls_salesforce       --properties "@adf/linkedService/ls_salesforce.json"

# datasets
az datafactory dataset create --factory-name $ADF -g $RG --name ds_landing_parquet   --properties "@adf/dataset/ds_landing_parquet.json"
az datafactory dataset create --factory-name $ADF -g $RG --name ds_sql_server_table  --properties "@adf/dataset/ds_sql_server_table.json"
az datafactory dataset create --factory-name $ADF -g $RG --name ds_salesforce_object --properties "@adf/dataset/ds_salesforce_object.json"

# pipelines
az datafactory pipeline create --factory-name $ADF -g $RG --name pl_extract_to_landing  --pipeline "@adf/pipeline/pl_extract_to_landing.json"
az datafactory pipeline create --factory-name $ADF -g $RG --name pl_master_etl_pipeline --pipeline "@adf/pipeline/pl_master_etl_pipeline.json"
```

**Update the notebook paths** in `pl_extract_to_landing.json` and
`pl_master_etl_pipeline.json` — they are absolute and currently point at the original
author's workspace (`/Workspace/Users/<upn>/azure-forecast-project/databricks/notebooks/...`).

> `ls_keyvault.json`, `ls_azure_sql_audit.json`, `ds_azure_sql_metadata.json`, and
> `ds_landing_csv.json` remain in the repo from earlier revisions. **Do not import them** —
> no active pipeline references them.

**✓** All imported linked services pass "Test connection" in ADF Studio.

---

## 7. Smoke test

Run **monthly first** — it is the smallest subject.

1. Deactivate the other three so the run is isolated:
   ```sql
   UPDATE hpe_catalog.audit.pipeline_config
   SET    is_active = false WHERE data_subject <> 'o9_forecast_monthly';
   ```
2. **Debug** `pl_extract_to_landing` in ADF Studio.
3. Confirm Parquet appears under `landing/o9/monthly/`.
4. Watch `pl_master_etl_pipeline`: `01_landing_to_silver` → `03_silver_to_gold` →
   `04_aggregated_audit`.
5. Verify:

```sql
SELECT layer, status, records_inserted, records_updated, error_records, error_message
FROM   hpe_catalog.audit.job_log
WHERE  run_id = '<adf-run-id>' ORDER BY insert_time;

SELECT count(*) FROM hpe_catalog.gold.dim_product;    -- expect exactly 500
SELECT count(*) FROM hpe_catalog.gold.dim_location;   -- expect exactly 200

SELECT (SELECT count(*) FROM hpe_catalog.gold.v_forecast_star) AS star_rows,
       (SELECT count(*) FROM hpe_catalog.gold.fact_forecast)   AS fact_rows;
```

**✓** Three `SUCCESS` rows sharing one `run_id`; `dim_product` = 500, `dim_location` = 200;
star and fact counts match. **Do not proceed until those two numbers are exact** — they are
what proves the seed data is keyed correctly.

Then reactivate the other subjects and repeat for daily, weekly, quarterly.

---

## 8. Activate schedules

```powershell
az datafactory trigger create --factory-name $ADF -g $RG --name tr_daily_schedule --properties "@adf/trigger/tr_daily_schedule.json"
az datafactory trigger start  --factory-name $ADF -g $RG --name tr_daily_schedule
# repeat for tr_weekly_schedule, tr_monthly_schedule, tr_quarterly_schedule
```

| Trigger | Schedule (UTC) | Subject |
|---|---|---|
| `tr_daily_schedule` | 06:00 daily | `o9_forecast_daily` |
| `tr_weekly_schedule` | Mon 07:00 | `o9_forecast_weekly` |
| `tr_monthly_schedule` | 1st 08:00 | `o9_forecast_monthly` |
| `tr_quarterly_schedule` | Quarter start 09:00 | `o9_forecast_quarterly` |

**✓** All four show **Started** under Manage → Triggers.

---

## 9. Validate and harden

```powershell
pip install pyspark pytest
pytest tests/ -v
```

Then:

- Unity Catalog lineage shows `silver → gold`.
- `data_quality_log` shows non-zero failures for the seeded dirty rows (`HARDWARE`,
  `UNKNOWN`, `MISC` categories; `XYZ` currency).
- A read-only analyst can `SELECT` Gold but not Silver:
  ```sql
  GRANT USAGE  ON CATALOG hpe_catalog      TO `analyst-group`;
  GRANT USAGE  ON SCHEMA  hpe_catalog.gold TO `analyst-group`;
  GRANT SELECT ON SCHEMA  hpe_catalog.gold TO `analyst-group`;
  ```

---

## Rebuilding an existing environment

To reload the medallion without re-provisioning:

1. Re-seed the sources (§5).
2. Re-extract via `pl_extract_to_landing`.
3. [sql/10_reset_medallion.sql](sql/10_reset_medallion.sql), then
   [data_model/03_gold_schema.sql](data_model/03_gold_schema.sql).
4. Run per subject, monthly first: `01_landing_to_silver` → `03_silver_to_gold` →
   `04_aggregated_audit`.
5. Once Silver is verified across subjects, [sql/13_drop_bronze_layer.sql](sql/13_drop_bronze_layer.sql)
   removes the legacy Bronze schema.

Landing and archive are never touched by a reset, so a replay never re-reads the sources.

---

## Order at a glance

```
Provision  →  Unity Catalog + DDL  →  Secret scope  →  Cluster (Single User + ngdbc.jar)
    →  Seed sources  →  ADF (SHIR → LS → datasets → pipelines)
    →  Smoke test (monthly first, verify 500/200)  →  Start triggers  →  Tests + grants
```

## Common gotchas

- **JAR install fails on the cluster.** Access mode must be **Single User**; UC's artifact
  allowlist blocks JARs on Shared clusters.
- **SAP HANA TLS errors.** The free `HDB_CLIENT_NO_CRYPTO` Windows client cannot do TLS —
  that is why HANA is read over JDBC from Databricks rather than by an ADF Copy activity.
- **`Cannot connect to jdbc:sap://…` / `Object is closed: SecureChannelSession`.** The HANA
  Cloud instance is stopped. BTP trial instances auto-stop daily. Port 443 still accepts the
  TCP connection (the load balancer answers), but the TLS handshake is dropped immediately,
  so the driver reports a closed secure channel rather than a refused connection. Start the
  instance in BTP Cockpit → SAP HANA Cloud → ⋯ → Start, wait for **Running**, then confirm
  with `python sql/check_hana_load.py`. This is not a cluster, driver or notebook fault —
  `openssl s_client -connect <host>:443` fails the same way from any machine.
- **ADF notebook paths are absolute** and point at the original author's workspace. Update
  them after import or every notebook activity 404s.
- **Secret scope name** is `hpe-forecast`, case-sensitive. `kv-scope` is from an older
  revision and no longer exists.
- **`dim_product` above 500 rows** means a dimension attribute is changing per row instead
  of per product. Check the seed scripts, not the pipeline (§5).
- **`batch_id` matched 0 rows** in `04_aggregated_audit` means Silver's batch id was passed
  instead of Gold's. The loud failure is intentional.
- **Do not run** `data_model/01_bronze_schema.sql` on a new deployment.
