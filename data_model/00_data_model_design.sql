-- ============================================================
-- DATA MODEL DOCUMENTATION
-- Star Schema Design for o9 Forecast Data
-- ============================================================

/*
============================================================
MEDALLION ARCHITECTURE OVERVIEW
============================================================

┌─────────────────────────────────────────────────────────────┐
│                    LANDING ZONE (ADLS)                       │
│  Raw CSV files from source system (o9)                      │
│  Pattern: landing/o9/<frequency>/*.csv                      │
│  Frequencies: daily, weekly, monthly, quarterly             │
└────────────────────────┬────────────────────────────────────┘
                         │ ADF + Databricks
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    BRONZE (Raw Layer)                        │
│  - 1:1 copy of source data                                  │
│  - All columns as STRING (schema-on-read)                   │
│  - Added: audit columns (_file_name, _ingestion_ts, etc.)   │
│  - Added: _frequency column (daily/weekly/monthly/quarterly)│
│  - Write mode: OVERWRITE (full refresh)                     │
│  - Format: Delta                                            │
└────────────────────────┬────────────────────────────────────┘
                         │ Data Quality Checks (Null PK validation)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    SILVER (Reference Layer)                  │
│  - Properly typed columns (DATE, DECIMAL, etc.)             │
│  - Null/empty PK rows removed                               │
│  - Empty strings → NULL                                     │
│  - Write mode: APPEND (historical)                          │
│  - Partitioned by: _ingestion_date                          │
│  - Format: Delta                                            │
└────────────────────────┬────────────────────────────────────┘
                         │ Business transformation (period derivation)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    GOLD (Business Layer)                     │
│  - Business-ready dimension table                           │
│  - Period column derived from partition_column              │
│  - Star schema (Fact + Dimensions)                          │
│  - Aggregated audit/KPI table                              │
│  - Write mode: OVERWRITE (dimension), APPEND (agg_audit)   │
│  - Partitioned by: period                                   │
│  - Format: Delta                                            │
└─────────────────────────────────────────────────────────────┘


============================================================
STAR SCHEMA DESIGN
============================================================

                    ┌──────────────┐
                    │  dim_time    │
                    │  (date_key)  │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴───────────────────┐    ┌──────────────────┐
│ dim_product  │────│  fact_o9_forecast_dmnsn   │────│  dim_location    │
│ (product_id) │    │  (product_id,             │    │  (location_id)   │
└──────────────┘    │   location_id,            │    └──────────────────┘
                    │   forecast_date,          │
                    │   forecast_qty,           │
                    │   revenue_amount,         │
                    │   frequency,              │
                    │   period)                 │
                    └──────────────────────────┘


============================================================
MEDALLION ARCHITECTURE LAYER MAPPING
============================================================

Data Layer             →  Purpose                →  Delta Table
─────────────────────────────────────────────────────────────
Bronze (Raw)           →  Raw ingestion          →  bronze.o9_forecast_raw
Silver (Reference)     →  Cleansed/validated     →  silver.o9_forecast_ref
Gold (Dimension)       →  Business-ready         →  gold.o9_forecast_dmnsn
Gold (Agg Audit)       →  KPI summary            →  gold.o9_forecast_agg_audit
Azure SQL              →  Audit/Metadata         →  audit.pipeline_audit


============================================================
DATA FLOW TRANSFORMATIONS
============================================================

Bronze → Silver:
  - Type casting (STRING → DATE, DECIMAL)
  - Null check on PK columns (product_id, location_id, forecast_date)
  - Empty string → NULL conversion
  - Invalid rows logged to audit.data_quality_log

  Silver → Gold:
    - Period column derivation:
      IF apply_concat = true:
        period = concat(substring(forecast_date, 1, 7), "-01")  → "2024-01-01"
      ELSE:
        period = forecast_date
    - Partition by period for query performance

Gold → Agg Audit:
  - Group by file_name (fl_nm)
  - For each metric column: compute SUM
  - Transpose (stack) metrics into keyfigure/total_qty_amount rows
  - Count records per group

*/
