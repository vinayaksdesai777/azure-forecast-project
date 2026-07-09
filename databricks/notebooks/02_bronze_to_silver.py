# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze to Silver
# MAGIC Orchestrates: type cast → silver meta → DQ domain checks → SCD2 merge → period agg.
# MAGIC All logic lives in utilities/ (Strategy pattern). This notebook is an orchestrator only.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from utilities.audit_helper    import write_audit_entry, mark_audit_failed, log_dq_result
from utilities.dq_checks       import DomainCheck, run_dq_checks
from utilities.transformations import (TypeCastTransform, AddSilverMetaTransform,
                                       DerivePeriodTransform, apply_transforms, apply_scd2_merge)

# COMMAND ----------

dbutils.widgets.text("data_subject", "", "Data Subject")
dbutils.widgets.text("batch_id",     "", "Batch ID from Bronze")
dbutils.widgets.text("run_id",       "", "ADF Pipeline Run ID")

data_subject      = dbutils.widgets.get("data_subject")
upstream_batch_id = dbutils.widgets.get("batch_id")
run_id            = dbutils.widgets.get("run_id")

# COMMAND ----------

metadata       = get_pipeline_metadata(data_subject)
source_system  = metadata["source_system"]
bronze_table   = metadata["bronze_table"]
silver_table   = metadata["silver_table"]
num_partitions = int(metadata.get("num_partitions") or DEFAULT_NUM_PARTITIONS)

batch_id    = get_batch_id(f"silver_{data_subject}")
load_job_nr = get_timestamp()

print(f"Source : {bronze_table} | Target : {silver_table} | batch_id : {batch_id}")

# COMMAND ----------

# MAGIC %md ## Read Bronze

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

# MAGIC %md ## Apply Transformations (Strategy Pipeline)

# COMMAND ----------

typed_df = apply_transforms(bronze_df, [
    TypeCastTransform(),
    AddSilverMetaTransform(batch_id=batch_id),
])

# COMMAND ----------

# MAGIC %md ## Run Domain DQ Checks — flag violations, keep all rows (Silver behaviour)

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

merge_keys   = ["product_id", "location_id", "forecast_date", "_frequency"]
tracked_cols = ["forecast_qty", "revenue_amount", "customer_id", "channel",
                "category", "sub_category", "region", "country", "currency", "uom"]

try:
    records_inserted, records_updated = apply_scd2_merge(
        spark=spark,
        incoming_df=typed_df,
        target_table=silver_table,
        merge_keys=merge_keys,
        tracked_cols=tracked_cols,
        num_partitions=num_partitions,
    )
except Exception as e:
    mark_audit_failed(spark, batch_id=batch_id, layer="silver", object_name=silver_table,
                      error_message=str(e), source_system=source_system,
                      data_subject=data_subject, run_id=run_id)
    raise

# COMMAND ----------

# MAGIC %md ## Period Aggregations → Silver Agg Table

# COMMAND ----------

agg_table  = f"{SILVER_SCHEMA}.o9_forecast_period_agg"
period_df  = apply_transforms(typed_df, [DerivePeriodTransform(date_col="forecast_date")])

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

# MAGIC %md ## Write Audit Entry

# COMMAND ----------

write_audit_entry(
    spark=spark, batch_id=batch_id, layer="silver", status="SUCCESS",
    records_inserted=records_inserted, records_updated=records_updated,
    error_records=cat_fail_count + curr_fail_count,
    source_system=source_system, data_subject=data_subject,
    object_name=silver_table, run_id=run_id
)

dbutils.notebook.exit(f'{{"batch_id":"{batch_id}","src_count":{src_count},"records_inserted":{records_inserted},"records_updated":{records_updated}}}')
