# System Design Document
## o9 Forecast Data Pipeline

**Status:** as-built
**Companion to:** [01_technical_specification.md](01_technical_specification.md)

---

## 1. Component Map

| Component | Type | Responsibility |
|---|---|---|
| `pl_extract_to_landing` | ADF pipeline | Reads config, fans out over active subjects, extracts each source to landing Parquet, advances the watermark |
| `pl_master_etl_pipeline` | ADF pipeline | Runs the medallion notebooks in sequence for one subject |
| `00_config` | Notebook | Catalog names, ADLS paths, secrets, config lookup, DQ domains |
| `00_get_metadata` | Notebook | Returns active `pipeline_config` rows to ADF as JSON |
| `00_update_watermark` | Notebook | Advances `last_watermark`, flips `load_type` to `incremental` |
| `01_ingest_hana_to_landing` | Notebook | SAP HANA → landing Parquet over JDBC |
| `01_landing_to_silver` | Notebook | Landing Parquet → Silver Delta (harmonize, DQ, cast, SCD2) |
| `03_silver_to_gold` | Notebook | Dimensions, surrogate keys, fact |
| `04_aggregated_audit` | Notebook | KPI aggregation and transposition |
| `utilities/dq_checks.py` | Module | DQ strategies + runner |
| `utilities/transformations.py` | Module | Transform strategies, SCD2 merges, SK helpers |
| `utilities/audit_helper.py` | Module | `write_audit_entry`, `mark_audit_failed`, `log_dq_result` |
| `shir-hpe-forecast` | Integration runtime | On-prem SQL Server connectivity |
| GitHub Actions | CI/CD | Validates ADF JSON and repo tests; deploys the target factory after the validation gate passes |

There is no Bronze component. `01_ingest_to_bronze` and `02_bronze_to_silver` were merged
into `01_landing_to_silver` in commit `654f3b3`; see the technical specification §3.1 for
the rationale.

---

## 2. Data Flow Detail

### 2.1 Extraction — `pl_extract_to_landing`

1. **Lookup** — calls `00_get_metadata`, which returns every `pipeline_config` row where
   `is_active = true`.
2. **ForEach** over the returned subjects.
3. **Switch** on `connector`:
   - `SapHana` → Databricks notebook activity running `01_ingest_hana_to_landing` (JDBC).
   - `SqlServer` → ADF Copy through `shir-hpe-forecast` to landing Parquet.
   - `Salesforce` → ADF Copy via the Salesforce connector to landing Parquet.
4. **Watermark** — on success, `00_update_watermark` reads back what landed, takes
   `MAX(watermark_column)` as the new high-watermark, and sets `load_type = 'incremental'`.
   The value comes from the landed data rather than `@utcNow()` so a row committed
   mid-extract is picked up next cycle instead of being stepped over. It runs inside the
   ForEach right after the extract, while the Silver load that clears landing only starts
   once the whole ForEach is done — so the files are still there to read.
5. **Chain** — invokes `pl_master_etl_pipeline` for the subject.

For `load_type = 'full'` the source query is unbounded. For `incremental`, it is bounded
below by `last_watermark` on the subject's `watermark_column`.

### 2.2 Landing → Silver — `01_landing_to_silver`

Runs as one notebook; the numbered stages below are its internal order.

1. **Read** landing Parquet from `abfss://landing@…/<landing_path>`, using a glob so
   zero-byte ADLS placeholder blobs are skipped. An empty read writes a `SUCCESS` audit
   row with zero counts and exits — a source with no new rows is not a failure.
2. **Harmonize** column names across the three sources.
3. **Enrich** — `AddAuditColumnsTransform` (frequency, load job number, batch id),
   `StandardizeTransform` (trim, upper-case on `category`).
4. **Structural DQ on raw strings** — `NullPKCheck` then `DedupCheck`. Failures route to
   quarantine. Running before the cast means a malformed value is described by the DQ
   layer rather than dying in a cast.
5. **Type cast** — `TypeCastTransform` applies the Silver contract
   (`DECIMAL(18,4)` quantities, `DECIMAL(18,2)` revenue, `DATE` forecast dates), then
   `AddSilverMetaTransform` stamps the Silver load metadata.
6. **Domain DQ** — `DomainCheck` on `category` and `currency`. Informational: rows are
   kept, violations logged.
7. **SCD2 merge** into `hpe_catalog.silver.o9_forecast_ref` via `apply_scd2_merge`,
   tracking `forecast_qty`, `revenue_amount`, and the descriptive attributes.
8. **Period aggregate** — `DerivePeriodTransform` rolls the batch up into
   `hpe_catalog.silver.o9_forecast_period_agg`.
9. **Archive and clear** — the files snapshotted in step 1 are copied to
   `archive/<data_subject>/<load_job_nr>/` and removed from landing, making landing a
   rolling window rather than an append-only pile. The clear is skipped unless every file
   arrived in the archive, and only snapshotted paths are removed, so a file dropped by an
   overlapping extract mid-run survives to the next cycle.
10. **Audit** — one `SUCCESS` row with insert/update/error counts.

### 2.3 Silver → Gold — `03_silver_to_gold`

1. Read the current Silver slice for this `_frequency`.
2. **`dim_product`** — build distinct product attributes, `build_surrogate_key` over
   `product_id`, then `apply_dim_scd2_merge`.
3. **`dim_location`** — same over `location_id`.
4. **`dim_time`** — derive calendar attributes; insert-only `MERGE` on `date_key`,
   because a calendar date's attributes never change.
5. **Resolve keys** — `resolve_dim_sk` joins the fact rows to the *current* dimension
   version to pick up `product_sk` and `location_sk`.
6. **Write fact** — `replaceWhere _frequency = '<freq>'`, so this subject rebuilds only
   its own slice of the shared table.
7. **Audit** — one `SUCCESS` row; the returned `batch_id` is what `04_aggregated_audit`
   must receive.

### 2.4 KPI aggregation — `04_aggregated_audit`

Reads `v_forecast_star` filtered to the upstream `batch_id`, aggregates by period /
frequency / category / region, then transposes the measures into `(keyfigure, kpi_value)`
rows using `stack()` built dynamically from the metric list. Writes
`hpe_catalog.gold.<subject>_agg_audit`.

If the passed `batch_id` matches zero rows it raises, listing the batch ids actually
present, rather than writing an empty success.

---

## 3. Schemas

### 3.1 `hpe_catalog.audit`

| Table | Purpose |
|---|---|
| `pipeline_config` | Per-subject runtime config; replaces the former Azure SQL `pipeline_metadata` |
| `job_log` | One row per layer per run |
| `data_quality_log` | One row per check per batch |

Key `pipeline_config` columns: `data_subject`, `source_system`, `connector`,
`source_schema`, `source_object`, `frequency`, `landing_path`, `silver_table`,
`gold_table`, `load_type`, `watermark_column`, `last_watermark`, `null_check_columns`,
`partition_column`, `num_partitions`, `is_active`.

`bronze_table` still exists on this table but is unused and read by nothing. It is kept
nullable so pre-existing config rows stay loadable.

### 3.2 `hpe_catalog.silver`

- `o9_forecast_ref` — typed, deduplicated, SCD2. Partitioned by `_ingestion_date`.
- `o9_forecast_period_agg` — pre-rolled period metrics.
- `quarantine` — rows rejected by structural DQ, with `_dq_fail_reason`.

### 3.3 `hpe_catalog.gold`

`dim_product`, `dim_location`, `dim_time`, `fact_forecast`, `v_forecast_star`, and one
`<subject>_agg_audit` table per data subject. See technical specification §6.3.

---

## 4. ADF Pipeline Interactions

```
tr_<freq>_schedule
        │
        ▼
pl_extract_to_landing ──► 00_get_metadata (config)
        │                 Switch(connector) ──► HANA JDBC notebook
        │                                  ──► Copy via SHIR (SQL Server)
        │                                  ──► Copy via Salesforce connector
        │                 00_update_watermark
        ▼
pl_master_etl_pipeline ──► 01_landing_to_silver
                       ──► 03_silver_to_gold
                       ──► 04_aggregated_audit
```

Within `pl_master_etl_pipeline` each notebook depends on its predecessor succeeding, so a
Silver failure prevents Gold from ever running against uncleansed data. The `batch_id`
returned by `03_silver_to_gold` is passed into `04_aggregated_audit` — passing Silver's
batch id instead is the classic wiring error, and the notebook fails loudly on it.

### 4.1 Triggers

| Trigger | Subject | Schedule (UTC) |
|---|---|---|
| `tr_daily_schedule` | `o9_forecast_daily` | 06:00 daily |
| `tr_weekly_schedule` | `o9_forecast_weekly` | 07:00 Mondays |
| `tr_monthly_schedule` | `o9_forecast_monthly` | 08:00 on the 1st |
| `tr_quarterly_schedule` | `o9_forecast_quarterly` | 09:00 quarterly |

All four fire **`pl_extract_to_landing`**, not `pl_master_etl_pipeline`. The chain runs
extract → master: a trigger pointed straight at the master pipeline would reprocess
whatever Parquet already sat in landing and never pull new rows from the source systems.

### 4.2 Release automation

The repository includes a deployment workflow that is part of the production control plane:

- CI validates the ADF JSON and the Python test suite before the push is considered releasable.
- The deploy workflow authenticates to Azure and creates or updates the target Data Factory artifacts after validation passes on `main`.
- A local release gate mirrors the same checks through `scripts/validate_release.ps1`.

This keeps the ADF definitions, the data tests, and the target environment aligned as a single release unit instead of depending on manual deployment steps.

---

## 5. Integration Runtime Routing

| Source | Runtime | Reason |
|---|---|---|
| SAP HANA Cloud | Databricks cluster (JDBC) | Avoids the ADF connector's ODBC/TLS driver constraint |
| SQL Server (on-prem) | `shir-hpe-forecast` | Private network reachability |
| Salesforce | Azure AutoResolveIntegrationRuntime | Public API endpoint |
| ADLS Gen2 | Azure AutoResolveIntegrationRuntime | Azure-internal |

---

## 6. Secret Routing

All secrets resolve from the Databricks secret scope **`hpe-forecast`**:

| Key | Consumer |
|---|---|
| `adls-account-name`, `adls-account-key` | `00_config.py`, notebook ADLS access |
| `hana-host`, `hana-user`, `hana-password` | `01_ingest_hana_to_landing` |
| `sqlserver-user`, `sqlserver-password` | `ls_sql_server` |
| `salesforce-user`, `salesforce-password`, `salesforce-token` | `ls_salesforce` |
| `sp-client-id`, `sp-client-secret`, `sp-tenant-id` | ADLS OAuth service principal |

`ls_keyvault.json` and `ls_azure_sql_audit.json` remain in `adf/` from earlier revisions
but are referenced by no active pipeline.

---

## 7. Error Handling

- Every notebook wraps its work so `mark_audit_failed` writes the audit row **before**
  the exception re-raises. ADF registers the failure; the diagnostic row persists.
- Batch ids are stage-prefixed (`silver_…`, `gold_…`, `agg_…`), so a failing row names
  its own layer without a join.
- An empty source read is a `SUCCESS` with zero counts, not a failure.
- A `batch_id` matching zero rows in `04_aggregated_audit` is a hard failure.
- Structural DQ failures do not stop the run; they are quarantined and counted. Domain
  violations do not stop the run at all.

---

## 8. Reset and Recovery

| Script | Effect |
|---|---|
| `sql/10_reset_medallion.sql` | Clears Silver and Gold state for all four subjects |
| `data_model/03_gold_schema.sql` | Recreates the Gold tables and the star view |
| `sql/13_drop_bronze_layer.sql` | Drops the legacy Bronze schema; run only after Silver is verified |

A reset touches neither landing nor archive. Note that landing is a rolling window that
the Silver load clears as it consumes, so after a normal run the history lives in the
`archive` container — a full replay re-reads from `archive`, never from the source
systems.
