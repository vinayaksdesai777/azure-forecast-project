# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Aggregated Audit (KPI Summary)
# MAGIC Reads today's Gold fact rows, aggregates KPIs by period/category/region,
# MAGIC and writes a summary to hpe_catalog.gold.<data_subject>_agg_audit.
# MAGIC No JDBC. No Azure SQL. All writes go to Unity Catalog Delta.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# sys.path for utilities/ is set by 00_config (%run above), derived from the
# notebook's own location so it works from any clone path.
from pyspark.sql import functions as F
from utilities.audit_helper import write_audit_entry, mark_audit_failed

# COMMAND ----------

dbutils.widgets.text("data_subject", "", "Data Subject")
dbutils.widgets.text("batch_id",     "", "Batch ID from Gold load")
dbutils.widgets.text("run_id",       "", "ADF Pipeline Run ID")

data_subject      = dbutils.widgets.get("data_subject")
upstream_batch_id = dbutils.widgets.get("batch_id")
run_id            = dbutils.widgets.get("run_id")

# COMMAND ----------

metadata       = get_pipeline_metadata(data_subject)
source_system  = metadata["source_system"]
gold_table     = metadata["gold_table"]
agg_audit_table = f"{GOLD_SCHEMA}.{data_subject.replace('o9_', '')}_agg_audit"

batch_id    = get_batch_id(f"agg_{data_subject}")
load_job_nr = get_timestamp()

print(f"Source      : {gold_table}")
print(f"Agg target  : {agg_audit_table}")
print(f"batch_id    : {batch_id}")

# COMMAND ----------

# MAGIC %md ## Read today's Gold data (current batch only)

# COMMAND ----------

try:
    gold_df = spark.table(gold_table)
except Exception as e:
    mark_audit_failed(spark, batch_id=batch_id, layer="agg_audit", object_name=agg_audit_table,
                      error_message=str(e), source_system=source_system,
                      data_subject=data_subject, run_id=run_id)
    raise

# Filter by the upstream batch_id so we only aggregate what this pipeline run loaded
if upstream_batch_id:
    today_df = gold_df.filter(F.col("_batch_id") == upstream_batch_id)
else:
    today_df = gold_df.filter(F.to_date(F.col("_gold_load_ts")) == F.current_date())

src_count = today_df.count()
print(f"Gold records for this batch: {src_count}")

if src_count == 0:
    print("No data for this batch. Exiting.")
    write_audit_entry(spark, batch_id=batch_id, layer="agg_audit", status="SUCCESS",
                      records_inserted=0, source_system=source_system,
                      data_subject=data_subject, object_name=agg_audit_table, run_id=run_id)
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# MAGIC %md ## Compute KPI aggregations

# COMMAND ----------

# Group by business dimensions; sum the two KPI measures; transpose to key-figure rows
GROUP_COLS  = ["period", "_frequency", "category", "region"]
METRIC_COLS = ["forecast_qty", "revenue_amount"]

# Validate columns exist (safe for schema evolution)
valid_groups  = [c for c in GROUP_COLS  if c in today_df.columns]
valid_metrics = [c for c in METRIC_COLS if c in today_df.columns]

agg_exprs = (
    [F.count(F.lit(1)).alias("no_of_records")]
    + [F.sum(F.col(c)).cast("decimal(22,4)").alias(c) for c in valid_metrics]
    + [F.max("_gold_load_ts").alias("load_ts")]
)

agg_df = today_df.groupBy(*valid_groups).agg(*agg_exprs)

# Transpose to one row per (group + keyfigure) — makes reporting queries trivial
if valid_metrics:
    stack_pairs  = ", ".join([f"'{c}', CAST(`{c}` AS DOUBLE)" for c in valid_metrics])
    stack_expr   = f"stack({len(valid_metrics)}, {stack_pairs}) AS (keyfigure, kpi_value)"
    transposed_df = (
        agg_df
        .selectExpr(*valid_groups, "no_of_records", "load_ts", stack_expr)
    )
else:
    transposed_df = agg_df.withColumn("keyfigure", F.lit("")).withColumn("kpi_value", F.lit(None).cast("double"))

result_df = (
    transposed_df
    .withColumn("data_subject", F.lit(data_subject))
    .withColumn("_batch_id",    F.lit(batch_id))
    .withColumn("_load_job_nr", F.lit(load_job_nr))
    .withColumn("ins_gmt_ts",   F.current_timestamp())
)

tgt_count = result_df.count()
print(f"Aggregated rows to write: {tgt_count}")

# COMMAND ----------

# MAGIC %md ## Write aggregated audit table

# COMMAND ----------

try:
    (
        result_df
        .repartition(1)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(agg_audit_table)
    )
    print(f"Written to: {agg_audit_table} | rows: {tgt_count}")
except Exception as e:
    mark_audit_failed(spark, batch_id=batch_id, layer="agg_audit", object_name=agg_audit_table,
                      error_message=str(e), source_system=source_system,
                      data_subject=data_subject, run_id=run_id)
    raise

# COMMAND ----------

# MAGIC %md ## Write audit entry

# COMMAND ----------

write_audit_entry(
    spark=spark, batch_id=batch_id, layer="agg_audit", status="SUCCESS",
    records_inserted=tgt_count, records_updated=0,
    source_system=source_system, data_subject=data_subject,
    object_name=agg_audit_table, run_id=run_id
)

dbutils.notebook.exit(f'{{"batch_id":"{batch_id}","src_count":{src_count},"agg_count":{tgt_count}}}')
