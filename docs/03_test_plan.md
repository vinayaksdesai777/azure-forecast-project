# Test Plan & Test Cases
## HPE o9 Forecast Data Pipeline

**Version:** 1.0  
**Date:** 2026-06-30  

---

## 1. QA Strategy

### 1.1 Testing Principles
- **Test at layer boundaries** — validate what enters and exits each medallion layer, not internal implementation details.
- **Dirty data is expected** — sources deliberately contain NULLs, negatives, duplicates, invalid categories/currencies. Tests must confirm these are handled correctly, not avoided.
- **Audit tables are the source of truth** — every test assertion for row counts and status uses `audit.pipeline_audit` and `audit.data_quality_log`.
- **Idempotency** — re-running any pipeline stage on the same input must produce the same output (no double-counting, no phantom records).

### 1.2 Test Levels

| Level | What | When |
|---|---|---|
| Unit | Individual DQ functions (`validate_not_null`, `validate_duplicates`, etc.) | Before any pipeline run |
| Integration | End-to-end layer (e.g., Bronze writes correct row count to Delta) | After each layer is deployed |
| System | Full pipeline run from source to Gold | After all components are deployed |
| Regression | Re-run after any code or config change | On every change |

### 1.3 Test Data
All tests use the **intentionally dirty source data** already loaded:
- SAP HANA: 9,800 rows (9,000 clean + 800 dirty)
- SQL Server: 9,800 rows (9,000 clean + 800 dirty)
- Salesforce: 9,800 × 2 = 19,600 rows (9,000 clean + 800 dirty each)

---

## 2. Unit Tests

### 2.1 `validate_not_null()` — data_quality.py

| Test ID | Scenario | Input | Expected |
|---|---|---|---|
| UT-01 | All PKs present | DataFrame with no NULLs in product_id, location_id, forecast_date | valid_df = all rows, invalid_df = empty |
| UT-02 | product_id NULL | 300 rows with product_id = NULL | invalid_df = 300 rows, valid_df = remainder |
| UT-03 | Multiple PK NULLs | Rows with NULLs across different PK columns | All NULL-PK rows in invalid_df |
| UT-04 | Empty string treated as NULL | product_id = '' | Treated as invalid after nullify_empty_strings() |

```python
# Example test (pytest)
def test_validate_not_null_filters_correctly(spark):
    data = [("P1","L1","2025-01-01"), (None,"L2","2025-01-01"), ("P3",None,None)]
    df = spark.createDataFrame(data, ["product_id","location_id","forecast_date"])
    valid, invalid = validate_not_null(df, ["product_id","location_id","forecast_date"])
    assert valid.count() == 1
    assert invalid.count() == 2
```

### 2.2 `validate_duplicates()` — data_quality.py

| Test ID | Scenario | Input | Expected |
|---|---|---|---|
| UT-05 | No duplicates | 100 unique rows | dedup_df = 100 rows |
| UT-06 | Exact duplicates | 200 rows with same (product_id, location_id, forecast_date, _frequency) | dedup_df keeps 1 per group, drops rest |
| UT-07 | Keep latest | Duplicates with different _ingestion_ts | Row with MAX(_ingestion_ts) is kept |

### 2.3 `nullify_empty_strings()` — data_quality.py

| Test ID | Scenario | Input | Expected |
|---|---|---|---|
| UT-08 | Empty string | category = '' | category = NULL |
| UT-09 | Whitespace only | customer_id = '   ' | customer_id = NULL |
| UT-10 | Valid value | channel = 'DIRECT' | channel unchanged |

### 2.4 `validate_data_types()` — data_quality.py

| Test ID | Scenario | Input | Expected |
|---|---|---|---|
| UT-11 | Valid date string | forecast_date = '2025-01-15' | Passes DATE cast |
| UT-12 | Invalid date string | forecast_date = 'not-a-date' | Flagged as invalid |
| UT-13 | Negative decimal | forecast_qty = '-100' | Flags if range check applied |

---

## 3. Integration Tests

### 3.1 Bronze Layer

| Test ID | What to Test | How to Verify | Expected |
|---|---|---|---|
| IT-01 | Row count after Bronze ingest | `SELECT COUNT(*) FROM hpe_catalog.bronze.o9_forecast_raw WHERE _frequency='daily'` | 9,800 rows |
| IT-02 | All columns are STRING | `DESCRIBE hpe_catalog.bronze.o9_forecast_raw` | All source columns show STRING type |
| IT-03 | Audit columns present | Query for `_file_name`, `_ingestion_ts`, `_batch_id` | Not NULL on all rows |
| IT-04 | Dirty data preserved | `SELECT COUNT(*) WHERE customer_id IS NULL` | 0 — NULLs not yet cleaned (Bronze is raw) |
| IT-05 | Quarantine captures bad PKs | `SELECT COUNT(*) FROM hpe_catalog.bronze.quarantine` | Equal to rows with NULL product_id OR location_id OR forecast_date |
| IT-06 | Audit status = SUCCESS | `SELECT job_status FROM audit.pipeline_audit WHERE data_layer='bronze' ORDER BY job_end_ts DESC LIMIT 1` | 'SUCCESS' |
| IT-07 | Source CSV archived | Check `archive/daily/<timestamp>/` in ADLS | File exists, landing folder empty |
| IT-08 | Idempotent re-run | Re-run Bronze on same input | Row count unchanged (overwrite mode) |

### 3.2 Silver Layer
> Silver receives pre-cleaned rows from Bronze (no NULL PKs, no duplicates). Silver's job is SCD2, business logic, aggregations.

| Test ID | What to Test | How to Verify | Expected |
|---|---|---|---|
| IT-09 | Row count in Silver | `SELECT COUNT(*) FROM hpe_catalog.silver.o9_forecast_ref WHERE is_active=true` | = Bronze valid row count |
| IT-10 | Types correctly cast | `DESCRIBE hpe_catalog.silver.o9_forecast_ref` | forecast_date=DATE, forecast_qty=DECIMAL, revenue_amount=DECIMAL |
| IT-11 | SCD2 columns present | `DESCRIBE hpe_catalog.silver.o9_forecast_ref` | Shows effective_from, effective_to, is_active |
| IT-12 | Invalid currency flagged but kept | `SELECT COUNT(*) FROM silver.o9_forecast_ref WHERE currency='XX'` > 0 AND `SELECT records_failed FROM audit.data_quality_log WHERE check_type='currency_validation'` > 0 | Rows exist in Silver; flagged in DQ log |
| IT-13 | Period aggregations written | `SELECT COUNT(*) FROM hpe_catalog.silver.o9_forecast_period_agg` | > 0 |
| IT-14 | Audit written to Unity Catalog | `SELECT job_status FROM hpe_catalog.audit.job_log WHERE layer='silver'` | 'SUCCESS' |

### 3.3 Gold Layer

| Test ID | What to Test | How to Verify | Expected |
|---|---|---|---|
| IT-17 | Fact table row count | `SELECT COUNT(*) FROM hpe_catalog.gold.o9_forecast_dmnsn WHERE period='2025-01-01'` | Matches Silver output for that period |
| IT-18 | Partitioning correct | `SHOW PARTITIONS hpe_catalog.gold.o9_forecast_dmnsn` | Partitions by (period, _frequency) exist |
| IT-19 | KPI aggregation populated | `SELECT COUNT(*) FROM hpe_catalog.gold.o9_forecast_agg_audit WHERE _load_ts > current_date` | > 0 |
| IT-20 | KPI values reasonable | `SELECT SUM(total_qty_amount) FROM gold.o9_forecast_agg_audit WHERE keyfigure='forecast_qty'` | Positive, non-zero |
| IT-21 | Audit status = SUCCESS | `SELECT job_status FROM audit.pipeline_audit WHERE data_layer='gold'` | 'SUCCESS' |

### 3.4 Extraction Pipeline

| Test ID | What to Test | How to Verify | Expected |
|---|---|---|---|
| IT-22 | SAP HANA extract lands CSV | Check `landing/o9/daily/` in ADLS | CSV file present with > 0 bytes |
| IT-23 | SQL Server extract lands CSV | Check `landing/o9/weekly/` | CSV file present |
| IT-24 | Salesforce monthly extract | Check `landing/o9/monthly/` | CSV file present |
| IT-25 | Watermark updated after HANA extract | `SELECT last_watermark FROM audit.source_extract_metadata WHERE source_system='SAP_HANA'` | Updated to max CHANGED_ON from extract |
| IT-26 | Incremental — no re-extraction | Run extract twice; check row count difference | Second run = 0 new rows (watermark prevents re-extract) |

---

## 4. System Tests

### 4.1 End-to-End Daily Run

| Test ID | Scenario | Steps | Expected |
|---|---|---|---|
| ST-01 | Full daily pipeline | Trigger pl_extract_to_landing (daily) → wait → check Gold | `o9_forecast_dmnsn` has new rows for today's period |
| ST-02 | Audit trail complete | After ST-01, query `audit.pipeline_audit` | 4 rows: bronze/silver/gold/agg_audit — all SUCCESS |
| ST-03 | No data scenario | Run pl_master_etl_pipeline with empty landing folder | IfCondition FALSE path: NO_DATA audit entry, no notebook runs |
| ST-04 | Failed notebook recovery | Manually break Silver notebook; run full pipeline | Bronze = SUCCESS, Silver = FAILED in audit, Gold not run |
| ST-05 | Dirty data end-to-end | Confirm dirty source rows are processed correctly through all layers | Bronze: raw dirty, Silver: cleaned, Gold: only valid records |

### 4.2 Incremental Load Tests

| Test ID | Scenario | Expected |
|---|---|---|
| ST-06 | Add 10 new rows to HANA after first run | Second extract picks up only 10 new rows |
| ST-07 | Update existing HANA row (CHANGED_ON bumped) | Updated row re-extracted; Silver dedup keeps latest version |
| ST-08 | Salesforce full reload | All 9,800 rows re-extracted; Silver dedup prevents duplicates in Gold |

---

## 5. Data Quality Test Cases

### 5.1 Dirty Data Handling Verification

| DQ Check | Source | Dirty Rows | Bronze Behaviour | Silver Behaviour |
|---|---|---|---|---|
| NULL customer_id | HANA, SF | 300 each | Kept (not a PK — passes through) | NULL allowed; not flagged |
| Negative forecast_qty | HANA, SF, SQL | 200 each | Kept (Bronze doesn't validate values) | Flagged in DQ log; row kept |
| Duplicate rows | All sources | 200 each | **Deduped in Bronze** — keep 1, drop rest | Receives deduped rows only |
| Invalid category (LEGACY_HW) | HANA | 100 | **UPPER()** applied in Bronze; value passed through | Flagged in DQ log (not in VALID_CATEGORIES); row kept |
| Mixed case category | SQL Server | 300 | **UPPER()** applied in Bronze → 'SERVER', 'STORAGE' etc. | Receives standardized uppercase values |
| NULL revenue_amount | SQL Server | 200 | Kept (not a PK — passes through) | NULL allowed; not flagged |
| NULL PK (product_id / location_id / forecast_date) | Any | varies | **Quarantined in Bronze** → bronze.quarantine | Never reaches Silver |
| Invalid currency (XX) | All sources | 100 each | Kept (Bronze doesn't validate values) | Flagged in DQ log; row kept in Silver |

### 5.2 Verification Queries

**After Silver run — confirm dirty data handled:**
```sql
-- 1. Confirm no mixed case in Silver
SELECT COUNT(*) as mixed_case_count
FROM hpe_catalog.silver.o9_forecast_ref
WHERE category != UPPER(category);
-- Expected: 0

-- 2. Confirm no duplicate PKs in Silver
SELECT product_id, location_id, forecast_date, _frequency, COUNT(*) as cnt
FROM hpe_catalog.silver.o9_forecast_ref
GROUP BY product_id, location_id, forecast_date, _frequency
HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- 3. Confirm invalid currency flagged in DQ log
SELECT check_type, records_failed
FROM audit.data_quality_log
WHERE check_type = 'currency_validation';
-- Expected: records_failed >= 100

-- 4. Confirm quarantine table has rows
SELECT COUNT(*) FROM hpe_catalog.bronze.quarantine;
-- Expected: > 0
```

---

## 6. Regression Test Checklist

Run after any code or configuration change:

- [ ] IT-01: Bronze row count matches source
- [ ] IT-06: Bronze audit status = SUCCESS
- [ ] IT-11: No NULL PKs in Silver
- [ ] IT-12: No duplicates in Silver
- [ ] IT-13: No mixed case in Silver
- [ ] IT-17: Gold fact table has expected rows
- [ ] IT-21: Gold audit status = SUCCESS
- [ ] ST-02: All 4 audit rows present after full run
- [ ] ST-03: NO_DATA path works on empty landing

---

## 7. Existing Automated Tests

Location: `tests/test_data_quality.py`

Covers:
- `validate_not_null()` with mock DataFrames
- `validate_duplicates()` key column deduplication
- `nullify_empty_strings()` edge cases
- `log_dq_results()` JDBC write (mocked)

Run with:
```bash
pytest tests/test_data_quality.py -v
```

---

## 8. Gap-Specific Test Cases

Tests that must pass once the implementation gaps (from Technical Spec Section 9) are resolved. These do not pass today — they define the acceptance criteria for each fix.

---

### 8.1 Landing — Parquet Format

| Test ID | Test | Expected |
|---|---|---|
| GAP-L01 | After extraction run, check file extension in `landing/o9/daily/` | Files end in `.parquet`, not `.csv` |
| GAP-L02 | Read landing file with `spark.read.parquet(path)` | Loads without error, correct column count |
| GAP-L03 | Salesforce full extract row count | `landing/o9/monthly/` file has 9,800 rows (not 200) |

```python
# GAP-L03 verification
df = spark.read.parquet("abfss://landing@<account>.dfs.core.windows.net/o9/monthly/")
assert df.count() == 9800, f"Expected 9800, got {df.count()} — LIMIT 200 bug still present"
```

---

### 8.2 Bronze — Standardization, PK Check, Dedup, Quarantine
> Bronze owns: column standardization, PK null check, exact dedup, quarantine routing, success/failure status.

| Test ID | Test | Expected |
|---|---|---|
| GAP-B01 | `category` column in `bronze.o9_forecast_raw` | All uppercase — no `'server'`, `'Storage'` |
| GAP-B02 | `bronze.quarantine` row count after run | = number of rows with NULL product_id OR location_id OR forecast_date in source |
| GAP-B03 | `bronze.o9_forecast_raw` has no NULL PKs | `SELECT COUNT(*) WHERE product_id IS NULL OR location_id IS NULL OR forecast_date IS NULL` = 0 |
| GAP-B04 | `bronze.o9_forecast_raw` has no exact duplicates | `GROUP BY product_id, location_id, forecast_date, _frequency HAVING COUNT(*) > 1` = 0 rows |
| GAP-B05 | Quarantine rows tagged with reason | `SELECT DISTINCT _dq_fail_reason FROM bronze.quarantine` = 'NULL_PK' |
| GAP-B06 | Audit written to Unity Catalog not Azure SQL | `SELECT * FROM hpe_catalog.audit.job_log WHERE layer='bronze'` returns rows |

```sql
-- GAP-B01: no mixed case after Bronze standardization
SELECT COUNT(*) FROM hpe_catalog.bronze.o9_forecast_raw
WHERE category != UPPER(category);
-- Expected: 0

-- GAP-B04: no duplicates after Bronze dedup
SELECT product_id, location_id, forecast_date, _frequency, COUNT(*) cnt
FROM hpe_catalog.bronze.o9_forecast_raw
GROUP BY product_id, location_id, forecast_date, _frequency
HAVING COUNT(*) > 1;
-- Expected: 0 rows
```

---

### 8.3 Silver — SCD Type 2, Business Logic, Aggregations
> Silver owns: SCD Type 2 history tracking, business rule validation, period aggregations.
> Silver does NOT do PK null check or dedup — those are Bronze responsibilities.

| Test ID | Test | Expected |
|---|---|---|
| GAP-S01 | Silver schema has SCD2 columns | `DESCRIBE hpe_catalog.silver.o9_forecast_ref` shows `effective_from`, `effective_to`, `is_active` |
| GAP-S02 | New record first insert | `is_active=true`, `effective_from=today`, `effective_to=NULL` |
| GAP-S03 | Changed record (e.g. category revised in source) | Old row: `is_active=false`, `effective_to=today`. New row: `is_active=true`, `effective_from=today` |
| GAP-S04 | Unchanged record re-ingested (idempotent) | Only 1 active row — no duplicate insert |
| GAP-S05 | `records_updated` in job_log > 0 after SCD2 merge with changes | `SELECT records_updated FROM hpe_catalog.audit.job_log WHERE layer='silver'` > 0 |
| GAP-S06 | Invalid currency flagged in DQ log (not dropped) | `audit.data_quality_log` has rows for currency_validation with records_failed > 0. Silver still contains the row. |
| GAP-S07 | Invalid category flagged in DQ log (not dropped) | Same as GAP-S06 for category_validation |
| GAP-S08 | Period aggregations written | `SELECT COUNT(*) FROM hpe_catalog.silver.o9_forecast_period_agg` > 0 after Silver run |

```sql
-- GAP-S03: verify SCD2 history for a changed product
SELECT product_id, category, effective_from, effective_to, is_active
FROM hpe_catalog.silver.o9_forecast_ref
WHERE product_id = 'HPE-PROD-1234'
ORDER BY effective_from;
-- Expected: 2 rows — one with is_active=false, one with is_active=true

-- GAP-S06: invalid currency rows still in Silver (flagged not dropped)
SELECT COUNT(*) FROM hpe_catalog.silver.o9_forecast_ref
WHERE currency = 'XX';
-- Expected: > 0 (rows kept, violation logged to DQ log only)
```

---

### 8.4 Gold — Periodic Fact, Dim Population, Archive

| Test ID | Test | Expected |
|---|---|---|
| GAP-G01 | Gold uses MERGE not overwrite | Run Gold twice on same Silver data — fact row count unchanged (no duplicates) |
| GAP-G02 | Historical periods not overwritten | Manually check a past period after a new run — old rows unchanged |
| GAP-G03 | `dim_product` populated | `SELECT COUNT(*) FROM hpe_catalog.gold.dim_product` > 0 |
| GAP-G04 | `dim_location` populated | `SELECT COUNT(*) FROM hpe_catalog.gold.dim_location` > 0 |
| GAP-G05 | `dim_time` populated | `SELECT COUNT(*) FROM hpe_catalog.gold.dim_time` > 0 |
| GAP-G06 | Archive table populated after Gold run | `SELECT COUNT(*) FROM hpe_catalog.gold.archive_o9_forecast` > 0 |
| GAP-G07 | Dead code bug fixed | `03_silver_to_gold.py` has no dangling write chain before the conditional block |

```sql
-- GAP-G01: idempotency check
-- Run Gold → note fact count
-- Run Gold again with same batch
-- Fact count must be identical
SELECT COUNT(*) FROM hpe_catalog.gold.fact_o9_forecast WHERE period = '2025-01-01';
-- Both runs: same number
```

---

### 8.5 Audit — Unity Catalog job_log

| Test ID | Test | Expected |
|---|---|---|
| GAP-A01 | job_log table exists in Unity Catalog | `SHOW TABLES IN hpe_catalog.audit` includes `job_log` |
| GAP-A02 | job_log has correct columns | `DESCRIBE hpe_catalog.audit.job_log` shows `records_inserted`, `records_updated` |
| GAP-A03 | After full pipeline run, 4 rows in job_log | One per layer: bronze, silver, gold, agg_audit |
| GAP-A04 | SCD2 merge populates `records_updated` correctly | Silver row shows `records_updated > 0` when changes detected |
| GAP-A05 | Azure SQL `pipeline_audit` no longer written | After implementation, verify no new rows in `audit.pipeline_audit` |

---

### 8.6 `00_config` Notebook

| Test ID | Test | Expected |
|---|---|---|
| GAP-C01 | File exists at `databricks/notebooks/00_config.py` | File present in repo |
| GAP-C02 | `get_batch_id()` returns unique UUID per call | Two calls return different values |
| GAP-C03 | `get_pipeline_metadata()` returns dict for valid data_subject | Returns metadata for `o9_forecast_daily` |
| GAP-C04 | `spark.sql("USE CATALOG hpe_catalog")` runs without error | No `CatalogNotFoundException` |
| GAP-C05 | All 4 notebooks run without `ModuleNotFoundError` on `%run ./00_config` | Clean run of each notebook |

---

### 8.7 Gap Test Summary

| Layer | Tests | Passes today |
|---|---|---|
| Landing (Parquet) | GAP-L01 to GAP-L03 | ❌ 0/3 |
| Bronze (standardize + quarantine) | GAP-B01 to GAP-B06 | ❌ 0/6 |
| Silver (SCD Type 2) | GAP-S01 to GAP-S05 | ❌ 0/5 |
| Gold (periodic fact + dims + archive) | GAP-G01 to GAP-G07 | ❌ 0/7 |
| Audit (Unity Catalog job_log) | GAP-A01 to GAP-A05 | ❌ 0/5 |
| 00_config notebook | GAP-C01 to GAP-C05 | ❌ 0/5 |
| **Total** | **31 tests** | **0 pass today** |

All 31 gap tests passing = implementation complete.
