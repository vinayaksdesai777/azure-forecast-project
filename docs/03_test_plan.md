# Test Plan & Test Cases
## o9 Forecast Data Pipeline

**Status:** as-built
**Companion to:** [01_technical_specification.md](01_technical_specification.md), [02_system_design.md](02_system_design.md)

---

## 1. QA Strategy

### 1.1 Principles

- **Verify counts, not vibes.** Every layer writes row counts to `job_log`; a test that
  does not compare a number against an expectation is not a test.
- **Dirty data is seeded deliberately.** The source scripts inject known violations, so
  the DQ layer has a fixed, predictable set of rows to catch.
- **Re-runs must be safe.** Silver merges and Gold `replaceWhere` are both idempotent —
  running the same batch twice must not change the final row counts.

### 1.2 Test levels

| Level | Scope | Where it runs |
|---|---|---|
| Unit | Pure DataFrame functions | `pytest tests/` — local, no Azure |
| Integration | One notebook against real Delta tables | Databricks |
| System | Full ADF run, extract → agg | Azure |
| Data quality | Seeded violations reach `data_quality_log` | Databricks / SQL |

### 1.3 Test data

Full load covers Jul 2020 – Jun 2025:

| Subject | Source | Rows |
|---|---|---|
| Daily | SAP HANA | ~547,800 |
| Quarterly | SAP HANA | 600,000 |
| Weekly | SQL Server | ~390,000 |
| Monthly | Salesforce | ~390,000 seeded, ~84,000 loaded (org storage cap) |

Seeded violations per source: `category ∈ {HARDWARE, UNKNOWN, MISC}` on `rn % 100 ∈ {0,1,2}`,
and `currency = 'XYZ'` on `rn % 67 = 0`.

---

## 2. Unit Tests

Run locally: `pip install pyspark pytest && pytest tests/ -v`

Existing coverage in `tests/test_data_quality.py`:

| Class | Function under test | Asserts |
|---|---|---|
| `TestValidateNotNull` | `validate_not_null()` | Null and empty-string PKs are separated into the invalid frame |
| `TestNullifyEmptyStrings` | `nullify_empty_strings()` | `""` becomes `NULL` so downstream `IS NULL` is reliable |
| `TestPeriodDerivation` | Period derivation | A date maps to the correct `YYYY-MM-01` period |
| `TestAggregation` | Measure aggregation | Sums group correctly by period |

### 2.1 Coverage gaps

These are not yet covered by automated tests and are verified manually in Databricks:

- `DedupCheck` — latest-row-per-key selection within a frequency partition
- `DomainCheck` — rows retained, violation counted
- `build_surrogate_key` — determinism across two runs on identical input
- `apply_scd2_merge` / `apply_dim_scd2_merge` — version close-out and new-version insert
- `resolve_dim_sk` — fact rows bind to the current dimension version

---

## 3. Integration Tests

### 3.1 Silver — `01_landing_to_silver`

| ID | Case | Expectation |
|---|---|---|
| SIL-01 | Run against a populated landing folder | `job_log` row: `layer='silver'`, `status='SUCCESS'`, non-zero `records_inserted` |
| SIL-02 | Run against an empty landing folder | `SUCCESS` with zero counts, no exception |
| SIL-03 | Null PK rows present | Rows in quarantine; `NULL_PK` row in `data_quality_log` with matching `records_failed` |
| SIL-04 | Duplicate business keys present | One row survives per key + frequency; `DEDUP` logged |
| SIL-05 | Invalid category present | Row **retained** in Silver; `CATEGORY_VALIDATION` logged with non-zero `records_failed` |
| SIL-06 | Invalid currency `XYZ` present | Row **retained**; `CURRENCY_VALIDATION` logged |
| SIL-07 | Re-run the same batch | Silver row count unchanged (merge is idempotent) |
| SIL-08 | Types after load | `forecast_qty` is `DECIMAL(18,4)`, `revenue_amount` `DECIMAL(18,2)`, `forecast_date` `DATE` |

### 3.2 Gold — `03_silver_to_gold`

| ID | Case | Expectation |
|---|---|---|
| GLD-01 | First load of one subject | `dim_product` = **500** rows, `dim_location` = **200** rows |
| GLD-02 | Second load of the same subject | Dimension counts **unchanged** — no SCD2 churn |
| GLD-03 | Load a second subject | Fact gains the new `_frequency` partition; existing partitions untouched |
| GLD-04 | Re-run one subject | Only that `_frequency` slice is rewritten (`replaceWhere`) |
| GLD-05 | Surrogate key integrity | No fact row has a `product_sk` or `location_sk` absent from its dimension |
| GLD-06 | Star view fan-out | `count(v_forecast_star)` equals `count(fact_forecast)` |

> **GLD-01 and GLD-06 are the critical pair.** Before commit `13c81a3` the seeds keyed
> `category` to `rn % 7` while `product_id` came from `rn % 500`; being coprime, every
> product cycled through all seven categories and Gold versioned the member on every
> load. `dim_product` reached 1,354 rows and the star view fanned out to 1,016,171 rows
> against 542,322 facts. Exactly 500 / 200 is the proof the fix holds.

### 3.3 Aggregation — `04_aggregated_audit`

| ID | Case | Expectation |
|---|---|---|
| AGG-01 | Correct upstream `batch_id` | `<subject>_agg_audit` populated; `agg_audit` row in `job_log` |
| AGG-02 | Wrong `batch_id` (e.g. Silver's) | **Raises**, listing the batch ids present. Must not write an empty success |
| AGG-03 | Transposition | Output is `(keyfigure, kpi_value)` rows, one per metric per group |

### 3.4 Extraction — `pl_extract_to_landing`

| ID | Case | Expectation |
|---|---|---|
| EXT-01 | Full load, `load_type='full'` | Landing Parquet written; row count matches source |
| EXT-02 | Watermark advance | `last_watermark` updated, `load_type` flips to `incremental` |
| EXT-03 | Incremental run | Only rows above the watermark extracted (~8–10k) |
| EXT-04 | `is_active = false` | Subject skipped entirely |
| EXT-05 | SHIR down | Weekly subject fails; other subjects unaffected |

---

## 4. System Tests

### 4.1 End-to-end single subject

Run monthly first — it is the smallest subject.

1. Seed the source.
2. Trigger `pl_extract_to_landing`.
3. Confirm landing Parquet exists.
4. Confirm Silver, Gold, and agg rows in `job_log` for the same `run_id`.
5. Confirm `dim_product` = 500, `dim_location` = 200.
6. Confirm `v_forecast_star` count equals `fact_forecast` count.

### 4.2 All four subjects

After all four have run: the fact holds four `_frequency` partitions, the dimensions are
still 500 / 200 (they are conformed — shared, not per-subject), and each subject has its
own `<subject>_agg_audit` table.

### 4.3 Failure propagation

Force a Silver failure. Expect: `job_log` has `status='FAILED'` with a populated
`error_message`, Gold never runs, and ADF reports the pipeline as failed.

---

## 5. Verification Queries

```sql
-- Run summary for one ADF run
SELECT layer, status, records_inserted, records_updated, error_records, error_message
FROM   hpe_catalog.audit.job_log
WHERE  run_id = '<adf-run-id>'
ORDER  BY insert_time;

-- The two numbers that prove the seed fix
SELECT count(*) AS dim_product_rows  FROM hpe_catalog.gold.dim_product;    -- expect 500
SELECT count(*) AS dim_location_rows FROM hpe_catalog.gold.dim_location;   -- expect 200

-- No product should carry more than one category
SELECT product_id, count(DISTINCT category) AS cats
FROM   hpe_catalog.gold.dim_product
GROUP  BY product_id
HAVING count(DISTINCT category) > 1;        -- expect zero rows

-- No location should span region/country pairs
SELECT location_id, count(DISTINCT concat(region, '|', country)) AS pairs
FROM   hpe_catalog.gold.dim_location
GROUP  BY location_id
HAVING count(DISTINCT concat(region, '|', country)) > 1;   -- expect zero rows

-- Star view must not fan out
SELECT (SELECT count(*) FROM hpe_catalog.gold.v_forecast_star) AS star_rows,
       (SELECT count(*) FROM hpe_catalog.gold.fact_forecast)   AS fact_rows;  -- must match

-- Orphaned surrogate keys
SELECT count(*) AS orphans
FROM   hpe_catalog.gold.fact_forecast f
LEFT   JOIN hpe_catalog.gold.dim_product p ON f.product_sk = p.product_sk
WHERE  p.product_sk IS NULL;                -- expect 0

-- DQ pass rates for a batch
SELECT check_type, records_checked, records_passed, records_failed,
       round(100.0 * records_passed / nullif(records_checked, 0), 2) AS pass_pct
FROM   hpe_catalog.audit.data_quality_log
WHERE  batch_id = '<batch-id>';

-- Fact partitions present
SELECT _frequency, count(*) AS rows
FROM   hpe_catalog.gold.fact_forecast
GROUP  BY _frequency ORDER BY _frequency;

-- Silver populated before dropping the legacy Bronze schema
SELECT _frequency, count(*) AS rows
FROM   hpe_catalog.silver.o9_forecast_ref
GROUP  BY _frequency ORDER BY _frequency;
```

---

## 6. Regression Checklist

Run after any change to the notebooks, the utilities, or the seed scripts:

- [ ] `pytest tests/` passes
- [ ] `dim_product` = 500, `dim_location` = 200
- [ ] No product with multiple categories; no location with multiple region/country pairs
- [ ] `v_forecast_star` count equals `fact_forecast` count
- [ ] Zero orphaned surrogate keys
- [ ] Re-running one subject leaves the other three `_frequency` partitions untouched
- [ ] `data_quality_log` shows non-zero failures for the seeded dirty rows
- [ ] A forced failure writes `status='FAILED'` with an `error_message` and stops the chain
- [ ] `04_aggregated_audit` raises on a mismatched `batch_id`

---

## 7. Post-Restart Verification

After a full re-seed and reload, verify in this order:

1. **Monthly first** — smallest subject, and `01_landing_to_silver` is a merged notebook
   assembled from two that each ran clean; the combination is worth exercising on the
   least data.
2. `job_log` shows `silver` → `gold` → `agg_audit`, all `SUCCESS`, same `run_id`.
3. `dim_product` = 500 and `dim_location` = 200. **This is the gate** — do not proceed to
   the other three subjects until both numbers are exact.
4. `v_forecast_star` count equals `fact_forecast` count.
5. Then daily, weekly, quarterly.
6. Only after Silver is verified across subjects: `sql/13_drop_bronze_layer.sql`.

Expect the monthly subject to be short of its seeded ~390,000 — the Salesforce Developer
Edition org caps at 10 MB, so roughly 84,000 of 108,000 records load. That is a source
constraint, not a pipeline defect, and the audit counts should reflect exactly what
arrived.
