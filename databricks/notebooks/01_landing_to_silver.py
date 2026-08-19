# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Landing to Silver
# MAGIC Landing Parquet → harmonize → structural DQ → type cast → domain DQ → SCD2 merge.
# MAGIC
# MAGIC Replaces the former 01_ingest_to_bronze + 02_bronze_to_silver pair. Landing is
# MAGIC the raw archive (immutable Parquet, copied to the archive container after each
# MAGIC load), so the separate Bronze Delta table added a hop without adding a
# MAGIC guarantee. Reprocessing still never touches the source systems — it re-reads
# MAGIC landing instead of Bronze.
# MAGIC
# MAGIC DQ ordering is deliberate and matches the previous two-notebook behaviour:
# MAGIC structural checks (null PK, duplicates) run on raw strings BEFORE casting, so a
# MAGIC malformed date is quarantined as a bad value rather than silently becoming NULL
# MAGIC and failing a null check for the wrong reason. Domain checks run after casting.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# sys.path for utilities/ is set by 00_config (%run above), derived from the
# notebook's own location so it works from any clone path.
from pyspark.sql import functions as F
from utilities.audit_helper    import write_audit_entry, mark_audit_failed
from utilities.dq_checks       import NullPKCheck, DedupCheck, DomainCheck, run_dq_checks
from utilities.transformations import (AddAuditColumnsTransform, StandardizeTransform,
                                       TypeCastTransform, AddSilverMetaTransform,
                                       DerivePeriodTransform, apply_transforms,
                                       apply_scd2_merge)

# COMMAND ----------

dbutils.widgets.text("data_subject", "", "Data Subject")
dbutils.widgets.text("source_path",  "", "Source Path Override")
dbutils.widgets.text("run_id",       "", "ADF Pipeline Run ID")

# .strip() — widget values keep whatever was pasted, including trailing spaces.
data_subject    = dbutils.widgets.get("data_subject").strip()
source_path_ovr = dbutils.widgets.get("source_path").strip()
run_id          = dbutils.widgets.get("run_id").strip()

# COMMAND ----------

metadata        = get_pipeline_metadata(data_subject)
source_system   = metadata["source_system"]
frequency       = metadata["frequency"]
silver_table    = metadata["silver_table"]
null_check_cols = [c.strip() for c in metadata.get("null_check_columns", "product_id,location_id,forecast_date").split(",")]
num_partitions  = int(metadata.get("num_partitions") or DEFAULT_NUM_PARTITIONS)

source_path = source_path_ovr if source_path_ovr else get_adls_path(CONTAINER_LANDING, metadata["landing_path"])
batch_id    = get_batch_id(f"silver_{data_subject}")
load_job_nr = get_timestamp()

print(f"Source  : {source_path}")
print(f"Target  : {silver_table} | frequency: {frequency}")
print(f"batch_id: {batch_id}")

# COMMAND ----------

# MAGIC %md ## Read Landing Parquet and Harmonize Column Names

# COMMAND ----------

_SF_COL_MAP = {
    "id": None,  # drop Salesforce system Id column
    "product_id__c": "product_id", "location_id__c": "location_id",
    "forecast_date__c": "forecast_date", "forecast_qty__c": "forecast_qty",
    "revenue_amount__c": "revenue_amount", "customer_id__c": "customer_id",
    "channel__c": "channel", "category__c": "category",
    "sub_category__c": "sub_category", "region__c": "region",
    "country__c": "country", "currency_code__c": "currency",
    "uom__c": "uom", "period_type__c": "_period_type",
    "fiscal_period__c": "_fiscal_period",
}

# Applied to every source after lower-casing. The three systems agree on the
# business columns but not on the period labels, the watermark, or their
# surrogate keys — and the SCD2 merge resolves every target column against the
# incoming DataFrame, so a single unmapped column fails the whole load with
# DELTA_MERGE_UNRESOLVED_EXPRESSION.
#
#   SQL Server  period_type / fiscal_period / modified_dt / forecast_id
#   SAP HANA    PERIOD_TYPE / FISCAL_PERIOD / CHANGED_ON
#   Salesforce  period_type__c / fiscal_period__c (renamed by _SF_COL_MAP above)
#
# Period labels keep the underscore prefix so they stay out of the business
# columns. The source watermark and surrogate key are dropped: silver keeps its
# own audit columns and SCD2 keys, and nothing downstream reads either.
_COMMON_COL_MAP = {
    "period_type":   "_period_type",
    "fiscal_period": "_fiscal_period",
    "changed_on":    None,   # HANA watermark
    "modified_dt":   None,   # SQL Server watermark
    "forecast_id":   None,   # SQL Server IDENTITY surrogate key
}

try:
    # Use glob to skip zero-byte ADLS placeholder blobs in the landing folder
    glob_path = source_path.rstrip("/") + "/*.parquet"
    raw_df = spark.read.format("parquet").load(glob_path)

    # Normalize column names (HANA returns UPPERCASE, Salesforce suffixes with __c)
    raw_df = raw_df.toDF(*[c.lower() for c in raw_df.columns])
    if source_system == "SALESFORCE":
        for old_col, new_col in _SF_COL_MAP.items():
            if old_col in raw_df.columns:
                raw_df = raw_df.drop(old_col) if new_col is None \
                         else raw_df.withColumnRenamed(old_col, new_col)

    # Then the cross-source map, so SQL Server and HANA land the same shape.
    for old_col, new_col in _COMMON_COL_MAP.items():
        if old_col in raw_df.columns:
            raw_df = (raw_df.drop(old_col) if new_col is None
                      else raw_df.withColumnRenamed(old_col, new_col))

    # Schema-on-read: everything lands as string so a source type change cannot
    # break ingestion. TypeCastTransform below decides what each column means.
    raw_df = raw_df.select([F.col(c).cast("string").alias(c) for c in raw_df.columns])
except Exception as e:
    mark_audit_failed(spark, batch_id=batch_id, layer="silver", object_name=silver_table,
                      error_message=str(e), source_system=source_system,
                      data_subject=data_subject, run_id=run_id)
    raise

src_count = raw_df.count()
print(f"Landing records: {src_count}")

if src_count == 0:
    write_audit_entry(spark, batch_id=batch_id, layer="silver", status="SUCCESS",
                      records_inserted=0, source_system=source_system,
                      data_subject=data_subject, object_name=silver_table, run_id=run_id)
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# MAGIC %md ## Add Audit Columns and Standardize

# COMMAND ----------

enriched_df = apply_transforms(raw_df, [
    AddAuditColumnsTransform(frequency=frequency, load_job_nr=load_job_nr, batch_id=batch_id),
    StandardizeTransform(upper_cols=["category"]),
])

# COMMAND ----------

# MAGIC %md ## Structural DQ — null PKs and duplicates (on raw strings, pre-cast)

# COMMAND ----------

# Quarantined rows are removed from the flow. Running this before the type cast
# keeps quarantined values in their original form for investigation.
clean_df, total_quarantined, struct_results = run_dq_checks(
    spark=spark,
    df=enriched_df,
    checks=[
        NullPKCheck(pk_cols=null_check_cols),
        DedupCheck(pk_cols=null_check_cols),
    ],
    batch_id=batch_id,
    layer="silver",
    table_name=silver_table,
    data_subject=data_subject,
    quarantine_table=f"{SILVER_SCHEMA}.quarantine",
)

dup_count  = next((r["records_failed"] for r in struct_results if r["check_type"] == "DEDUP"), 0)
null_count = next((r["records_failed"] for r in struct_results if r["check_type"] == "NULL_PK"), 0)
print(f"After structural DQ — clean: {clean_df.count()} | quarantined: {total_quarantined}")

# COMMAND ----------

# MAGIC %md ## Type Cast and Add Silver Metadata

# COMMAND ----------

typed_df = apply_transforms(clean_df, [
    TypeCastTransform(),
    AddSilverMetaTransform(batch_id=batch_id),
])

# COMMAND ----------

# MAGIC %md ## Domain DQ — flag violations, keep all rows (Silver behaviour)

# COMMAND ----------

_, _, dq_results = run_dq_checks(
    spark=spark,
    df=typed_df,
    checks=[
        DomainCheck(column="category", valid_values=VALID_CATEGORIES),
        DomainCheck(column="currency", valid_values=VALID_CURRENCIES),
    ],
    batch_id=batch_id,
    layer="silver",
    table_name=silver_table,
    data_subject=data_subject,
    quarantine_table=None,  # Domain checks are informational — no quarantine
)

cat_fail_count  = next((r["records_failed"] for r in dq_results if "CATEGORY" in r["check_type"]), 0)
curr_fail_count = next((r["records_failed"] for r in dq_results if "CURRENCY" in r["check_type"]), 0)

# COMMAND ----------

# MAGIC %md ## SCD Type 2 Merge into Silver

# COMMAND ----------

merge_keys   = ["product_id", "location_id", "forecast_date", "channel", "customer_id", "_frequency"]
tracked_cols = ["forecast_qty", "revenue_amount",
                "category", "sub_category", "region", "country", "currency", "uom"]

try:
    silver_path = get_adls_path("silver", data_subject)
    records_inserted, records_updated = apply_scd2_merge(
        spark=spark,
        incoming_df=typed_df,
        target_table=silver_table,
        merge_keys=merge_keys,
        tracked_cols=tracked_cols,
        num_partitions=num_partitions,
        storage_path=silver_path,
    )
except Exception as e:
    mark_audit_failed(spark, batch_id=batch_id, layer="silver", object_name=silver_table,
                      error_message=str(e), source_system=source_system,
                      data_subject=data_subject, run_id=run_id)
    raise

# COMMAND ----------

# MAGIC %md ## Period Aggregations → Silver Agg Table

# COMMAND ----------

agg_table = f"{SILVER_SCHEMA}.o9_forecast_period_agg"
period_df = apply_transforms(typed_df, [DerivePeriodTransform(date_col="forecast_date")])

(
    period_df
    .groupBy("period", "_frequency", "category", "region")
    .agg(
        F.sum("forecast_qty").cast("decimal(18,4)").alias("total_forecast_qty"),
        F.sum("revenue_amount").cast("decimal(18,2)").alias("total_revenue_amount"),
        F.count("*").alias("record_count"),
    )
    .withColumn("_batch_id",    F.lit(batch_id))
    .withColumn("_agg_load_ts", F.current_timestamp())
    .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(agg_table)
)
print(f"Period agg written to: {agg_table}")

# COMMAND ----------

# MAGIC %md ## Archive Landing + Audit

# COMMAND ----------

# Landing is the raw archive now that Bronze is gone: copy before anything can
# overwrite the folder on the next extract.
dbutils.fs.cp(source_path, get_adls_path(CONTAINER_ARCHIVE, f"{data_subject}/{load_job_nr}/"), recurse=True)

write_audit_entry(
    spark=spark, batch_id=batch_id, layer="silver", status="SUCCESS",
    records_inserted=records_inserted, records_updated=records_updated,
    error_records=total_quarantined + cat_fail_count + curr_fail_count,
    source_system=source_system, data_subject=data_subject,
    object_name=silver_table, run_id=run_id
)

dbutils.notebook.exit(
    f'{{"batch_id":"{batch_id}","src_count":{src_count},'
    f'"records_inserted":{records_inserted},"records_updated":{records_updated},'
    f'"quarantine_count":{null_count},"dup_count":{dup_count}}}'
)
