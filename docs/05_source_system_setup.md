# Source System Setup

`DEPLOYMENT_GUIDE.md` §5 says "run the seed script". This document is how you
get something to run it *against*. Three source systems, none of them Azure,
each with its own account, its own free tier, and its own way of failing.

Work through them in any order — they are independent — but finish all three
before §5, because the seed scripts assume the databases already exist.

| Source | What you need | Free? | Time |
|---|---|---|---|
| SAP HANA Cloud | BTP trial account | yes, 90 days | ~45 min |
| SQL Server | local install or Azure SQL | yes | ~30 min |
| Salesforce | Developer Edition org | yes, permanent | ~20 min |

Everything below produces credentials. All of them go into the Databricks secret
scope (`DEPLOYMENT_GUIDE.md` §3) or an ADF linked service — never into a file in
this repo.

---

## 1. SAP HANA Cloud

Feeds the **daily** and **quarterly** subjects. Two tables in one schema.

### 1.1 Create the BTP trial

1. Sign up at <https://www.sap.com/products/technology-platform/trial.html>.
   Use an account you will keep — the trial is tied to it and cannot be moved.
2. Choose a region when prompted. **US East (VA)** is the default used here;
   the JDBC hostname embeds the region, so note which one you pick.
3. In BTP Cockpit, open your subaccount → **Service Marketplace** →
   **SAP HANA Cloud** → **Create**.
4. Instance type **SAP HANA Cloud, SAP HANA Database**. Memory 30 GB is the
   trial default and is ample: the full seed is ~1.2 M rows.
5. Set the **DBADMIN password** and record it. There is no recovery — resetting
   it later means updating the Databricks secret and the ADF linked service to
   match.
6. Under **Connections**, set *Allow all IP addresses* (`0.0.0.0/0`), or the
   Databricks cluster cannot reach it.

### 1.2 Grant yourself the admin role

A fresh trial user often cannot open HANA Cloud Central at all — the tile opens
and reports "Not authorized — missing roles".

BTP Cockpit → **Security** → **Users** → your user → **Assign Role Collection**
→ `SAP HANA Cloud Administrator`. Sign out and back in.

### 1.3 Create the schema

Open **SAP HANA Cloud Central** → your instance → **Open in SQL Console**, then:

```sql
CREATE SCHEMA "O9_SOURCE";
```

The seed scripts create the tables themselves.

### 1.4 Record the connection details

From HANA Cloud Central, copy the **SQL endpoint**. It looks like:

```
<guid>.hna1.prod-us10.hanacloud.ondemand.com:443
```

| Secret key | Value |
|---|---|
| `saphana-host` | the hostname, without `:443` |
| `saphana-user` | `DBADMIN` |
| `saphana-password` | what you set in 1.1 |

### 1.5 The trial stops every day

**This will catch you out.** A BTP trial instance auto-stops on a daily
schedule. A stopped instance still accepts a TCP connection on 443 — the load
balancer answers — and then drops the TLS handshake, so the JDBC driver reports

```
Cannot connect to jdbc:sap://…  [Object is closed: …SecureChannelSession…]
```

which reads like a driver fault rather than a stopped database. Start it in BTP
Cockpit → SAP HANA Cloud → ⋯ → **Start** and wait for *Running*.

Confirm from anywhere with:

```powershell
$env:HANA_PASSWORD="<DBADMIN password>"
python sql/check_hana_load.py
```

That script also reports row counts and duplicate business keys, so it doubles
as the post-seed verification.

> A production HANA runs continuously. The daily stop is a trial artefact — do
> not design schedules around it.

---

## 2. SQL Server

Feeds the **weekly** subject. One table, `HPE_SOURCE.dbo.Forecast`.

Two options. Pick **2a** if you want to exercise the self-hosted integration
runtime (closer to a real on-prem source); pick **2b** if you would rather not
run anything locally.

### 2a. Local SQL Server + self-hosted IR

1. Install **SQL Server Developer Edition** (free) and **SSMS**.
2. Create the database:

   ```sql
   CREATE DATABASE HPE_SOURCE;
   ```

3. Enable **SQL Server Authentication** (Server Properties → Security → *SQL
   Server and Windows Authentication mode*), then create a login for ADF:

   ```sql
   USE HPE_SOURCE;
   CREATE LOGIN adf_reader WITH PASSWORD = '<strong-password>';
   CREATE USER adf_reader FOR LOGIN adf_reader;
   ALTER ROLE db_datareader ADD MEMBER adf_reader;
   ```

4. Install the **Self-hosted Integration Runtime** on the same machine and
   register it with the auth key from `DEPLOYMENT_GUIDE.md` §6a.

> **The SHIR is only as available as the machine it runs on.** On a laptop it
> goes offline whenever the machine sleeps, and every scheduled run that needs
> it fails. Only `ls_sql_server` genuinely requires it — the Databricks, ADLS
> and HANA linked services use the AutoResolve runtime, so a sleeping machine
> breaks the weekly subject alone. For anything you actually rely on, put the
> SHIR on an always-on VM.

### 2b. Azure SQL Database (no SHIR needed)

```powershell
az sql server create -n sql-hpe-forecast -g rg-hpe-forecast-dev `
  -l eastus -u sqladmin -p "<strong-password>"
az sql db create -n HPE_SOURCE -s sql-hpe-forecast -g rg-hpe-forecast-dev `
  --service-objective Basic
az sql server firewall-rule create -n AllowAzure -s sql-hpe-forecast `
  -g rg-hpe-forecast-dev --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0
```

Then set `ls_sql_server`'s `connectVia` to the AutoResolve runtime instead of
`shir-hpe-forecast`, and skip §6a entirely.

### 2c. Credentials

| Secret key | Value |
|---|---|
| `sqlserver-user` | `adf_reader` (or `sqladmin` for Azure SQL) |
| `sqlserver-password` | the password you set |

### 2d. A note on the seed

`sql/06_sqlserver_weekly_data.sql` inserts 390,000 rows in one statement. On a
small instance that can sit in `RESOURCE_SEMAPHORE` waiting for a memory grant
rather than failing outright — the query simply never finishes. If that happens,
cancel it and run the insert in batches; `sql/09_sqlserver_weekly_delta.sql`
shows the pattern.

---

## 3. Salesforce

Feeds the **monthly** subject.

### 3.1 Create a Developer Edition org

Sign up at <https://developer.salesforce.com/signup>. Free, permanent, no card.
You will be given a username of the form `you@example.com.dev` — that, not your
email, is the login.

### 3.2 Create the custom object

Deploy the metadata already in this repo:

```powershell
sf org login web --alias hpe-forecast-dev
sf project deploy start --source-dir salesforce/force-app --target-org hpe-forecast-dev
```

That creates `Forecast__c` with the 15 fields the pipeline expects. To do it by
hand instead, the field definitions are under
`salesforce/force-app/main/default/objects/Forecast__c/fields/`.

### 3.3 Get a security token

Setup → your avatar → **Settings** → **Reset My Security Token**. It arrives by
email. ADF needs it appended to the password.

| Secret key | Value |
|---|---|
| `salesforce-user` | `you@example.com.dev` |
| `salesforce-password` | your password |
| `salesforce-token` | the security token |

### 3.4 Developer Edition holds ~84,000 records, not 387,000

Data storage is capped. A Bulk API load of the full 60-month history stops
partway with:

```
STORAGE_LIMIT_EXCEEDED:storage limit exceeded
```

The project works around this: the **history is landed directly as Parquet**
into `landing/o9/monthly/`, and Salesforce carries only the ongoing monthly
refresh.

```powershell
# 60 months of history, written locally then uploaded to ADLS
$env:PYTHONIOENCODING="utf-8"
python salesforce/generate_full_load.py --dry-run
python salesforce/csv_to_landing_parquet.py --out-dir ./landing_out

$key = az storage account keys list -n <account> -g <rg> --query "[0].value" -o tsv
az storage fs directory upload -f landing --account-name <account> `
  --account-key $key -s "./landing_out/*" -d "o9/monthly" --recursive
```

Run these from the repo root — the scripts use relative paths — and set
`PYTHONIOENCODING`, because the generators print a `→` that the Windows console
codepage cannot encode.

> **Not Data Cloud.** An earlier revision pointed the monthly subject at a Data
> Cloud DLO (`HPE_Forecast_Monthly__dll`). ADF's Salesforce connector reads CRM
> objects over SOQL and Bulk API only; DLOs live in a separate lake reachable
> through the Data Cloud Query API, which no ADF Salesforce connector speaks.
> See <https://learn.microsoft.com/azure/data-factory/connector-salesforce-service-cloud>.
> It is reachable from ADF through a generic Web activity against that API, but
> that is a custom paginated sub-pipeline, not a Copy activity.

---

## 4. Verify before moving on

| Source | Check | Expect |
|---|---|---|
| HANA | `python sql/check_hana_load.py` | 2 tables, 500 products, 200 locations, 0 duplicate keys |
| SQL Server | `SELECT COUNT(*), MAX(modified_dt) FROM dbo.Forecast` | 390,000 and a max near the newest `forecast_date` |
| Salesforce | `sf data query --query "SELECT COUNT(Id) FROM Forecast__c" -o hpe-forecast-dev` | whatever you loaded |
| Landing | `az storage fs file list -f landing --path o9/monthly` | 60 Parquet files |

On `MAX(modified_dt)`: it must sit on the same timeline as `forecast_date`, not
at the wall-clock time you ran the seed. If history is stamped "now" while the
delta scripts stamp rows with the period they represent, the delta looks *older*
than the history and an incremental extract filtering `modified_dt > watermark`
matches nothing — reporting a clean run having pulled zero rows.

---

## 5. Where the credentials go

Nothing above belongs in this repository. `.gitignore` already excludes the
Salesforce CLI auth directory and the generated CSVs, but the discipline is
yours to keep.

```powershell
databricks secrets put-secret hpe-forecast saphana-host
databricks secrets put-secret hpe-forecast saphana-user
databricks secrets put-secret hpe-forecast saphana-password
databricks secrets put-secret hpe-forecast adls-account-name
databricks secrets put-secret hpe-forecast adls-account-key
```

ADF linked services hold their own credentials as `SecureString` values, set
through ADF Studio. `adf/linkedService/*.json` in this repo carries placeholders
like `<salesforce_security_token>` — a deployment overwrites the real value with
the placeholder if you are not careful, so set secrets in Studio *after*
running `scripts/deploy_adf.ps1`, or move them to Key Vault so the JSON only
carries a reference.
