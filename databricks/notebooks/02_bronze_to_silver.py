# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze to Silver
# MAGIC Reads Bronze (pre-cleaned: no NULL PKs, no exact dupes).
# MAGIC Applies type casting, business logic validation, SCD Type 2 MERGE,
# MAGIC and period-level aggregations.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from utilities.audit_helper import write_audit_entry, mark_audit_failed, log_dq_result

# COMMAND ----------

dbutils.widgets.text("data_subject", "", "Data Subject")
dbutils.widgets.text("batch_id",     "", "Batch ID from Bronze")
dbutils.widgets.text("run_id",       "", "ADF Pipeline Run ID")

data_subject     = dbutils.widgets.get("data_subject")
upstream_batch_id = dbutils.widgets.get("batch_id")
run_id           = dbutils.widgets.get("run_id")

# COMMAND ----------

metadata       = get_pipeline_metadata(data_subject)
source_system  = metadata["source_system"]
bronze_table   = metadata["bronze_table"]
silver_table   = metadata["silver_table"]
num_partitions = int(metadata.get("num_partitions") or DEFAULT_NUM_PARTITIONS)

batch_id    = get_batch_id(f"silver_{data_subject}")
load_job_nr = get_timestamp()

print(f"Source : {bronze_table}")
print(f"Target : {silver_table}")
print(f"batch_id: {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze (valid rows only — Bronze already guarantees no NULL PKs, no exact dupes)

# COMMAND ----------

bronze_df = spark.table(bronze_table)
src_count = bronze_df.count()
print(f"Bronze records: {src_count}")

if src_count == 0:
    write_audit_entry(spark, batch_id=batch_id, layer="silver", status="SUCCESS",
                      records_inserted=0, source_system=source_system,
                      data_subject=data_subject, object_name=silver_table, run_id=run_id)
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Type Casting + Empty String → NULL

# COMMAND ----------

from pyspark.sql.types import StringType

# Empty string → NULL for all string cols
typed_df = bronze_df
for col in [f.name for f in bronze_df.schema.fields if str(f.dataType) == "StringType()"]:
    typed_df = typed_df.withColumn(col, F.when(F.trim(F.col(col)) == "", None).otherwise(F.col(col)))

# Cast to business types
typed_df = (
    typed_df
    .withColumn("forecast_date",   F.to_date("forecast_date"))
    .withColumn("forecast_qty",    F.col("forecast_qty").cast("decimal(12,2)"))
    .withColumn("revenue_amount",  F.col("revenue_amount").cast("decimal(16,2)"))
    .withColumn("_ingestion_date", F.current_date())
    .withColumn("_silver_load_ts", F.current_timestamp())
    .withColumn("_batch_id",       F.lit(batch_id))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Logic Validation (flag violations, do NOT drop rows)

# COMMAND ----------

# Category validation
invalid_category = typed_df.filter(
    F.col("category").isNotNull() & ~F.col("category").isin(list(VALID_CATEGORIES))
)
cat_fail_count = invalid_category.count()

log_dq_result(spark, batch_id=batch_id, layer="silver",
              table_name=silver_table, check_type="CATEGORY_VALIDATION",
              column_name="category",
              records_checked=src_count, records_failed=cat_fail_count,
              data_subject=data_subject)

# Currency validation
invalid_currency = typed_df.filter(
    F.col("currency").isNotNull() & ~F.col("currency").isin(list(VALID_CURRENCIES))
)
curr_fail_count = invalid_currency.count()

log_dq_result(spark, batch_id=batch_id, layer="silver",
              table_name=silver_table, check_type="CURRENCY_VALIDATION",
              column_name="currency",
              records_checked=src_count, records_failed=curr_fail_count,
              data_subject=data_subject)

print(f"Business validation — invalid category: {cat_fail_count} | invalid currency: {curr_fail_count}")
print("Flagged rows kept in Silver (not dropped)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 MERGE into Silver

# COMMAND ----------

# SCD2 merge keys
merge_keys = ["product_id", "location_id", "forecast_date", "_frequency"]

# Columns that trigger a history update when changed
scd2_cols = ["forecast_qty", "revenue_amount", "customer_id", "channel",
             "category", "sub_category", "region", "country", "currency", "uom"]

# Add SCD2 columns to incoming data
new_df = (
    typed_df
    .withColumn("effective_from", F.current_date())
    .withColumn("effective_to",   F.lit(None).cast("date"))
    .withColumn("is_active",      F.lit(True))
)

records_inserted = 0
records_updated  = 0

if not spark.catalog.tableExists(silver_table):
    # First load — write directly
    (
        new_df
        .repartition(num_partitions)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("_ingestion_date")
        .saveAsTable(silver_table)
    )
    records_inserted = new_df.count()
    print(f"First load — inserted: {records_inserted}")
else:
    silver_dt = DeltaTable.forName(spark, silver_table)

    # Build merge condition on PK cols
    merge_condition = " AND ".join([f"existing.{k} = incoming.{k}" for k in merge_keys])

    # Build change detection condition across SCD2 tracked columns
    change_condition = " OR ".join([f"existing.{c} <> incoming.{c}" for c in scd2_cols
                                    if c in new_df.columns])

    # Step 1 — expire changed active rows
    silver_dt.alias("existing").merge(
        new_df.alias("incoming"),
        f"{merge_condition} AND existing.is_active = true AND ({change_condition})"
    ).whenMatchedUpdate(set={
        "effective_to": F.current_date(),
        "is_active":    F.lit(False)
    }).execute()

    # Count updated rows (those that got expired)
    records_updated = spark.table(silver_table).filter(
        (F.col("is_active") == False) & (F.col("effective_to") == F.current_date())
    ).count()

    # Step 2 — insert new/changed rows (not matched as active, or changed)
    silver_dt.alias("existing").merge(
        new_df.alias("incoming"),
        f"{merge_condition} AND existing.is_active = true AND NOT ({change_condition})"
    ).whenNotMatchedInsertAll().execute()

    records_inserted = new_df.count() - records_updated
    print(f"SCD2 merge — inserted: {records_inserted} | updated (expired): {records_updated}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Period Aggregations → Silver Agg Table

# COMMAND ----------

agg_table = f"{SILVER_SCHEMA}.o9_forecast_period_agg"

period_agg_df = (
    typed_df
    .withColumn("period", F.date_format(F.col("forecast_date"), "yyyy-MM-01"))
    .groupBy("period", "_frequency", "category", "region", "source_system" if "source_system" in typed_df.columns else F.lit(source_system).alias("source_system"))
    .agg(
        F.sum("forecast_qty").cast("decimal(18,4)").alias("total_forecast_qty"),
        F.sum("revenue_amount").cast("decimal(18,2)").alias("total_revenue_amount"),
        F.count("*").alias("record_count")
    )
    .withColumn("_batch_id",    F.lit(batch_id))
    .withColumn("_agg_load_ts", F.current_timestamp())
)

(
    period_agg_df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(agg_table)
)
print(f"Period aggregations written to: {agg_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Audit Entry

# COMMAND ----------

write_audit_entry(
    spark=spark, batch_id=batch_id, layer="silver", status="SUCCESS",
    records_inserted=records_inserted, records_updated=records_updated,
    error_records=cat_fail_count + curr_fail_count,
    source_system=source_system, data_subject=data_subject,
    object_name=silver_table, run_id=run_id
)

# COMMAND ----------

dbutils.notebook.exit(f'{{"batch_id":"{batch_id}","src_count":{src_count},"records_inserted":{records_inserted},"records_updated":{records_updated}}}')
