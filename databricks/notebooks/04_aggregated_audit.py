# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Aggregated Audit (KPI Summary)
# MAGIC 
# MAGIC Generates aggregated audit metrics from the Gold layer.
# MAGIC Aggregates KPI metrics and transposes using stack logic for reporting.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("data_subject", "", "Data Subject")
dbutils.widgets.text("batch_id", "", "Batch ID from Gold load")

data_subject = dbutils.widgets.get("data_subject")
upstream_batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

# Load metadata
metadata = get_pipeline_metadata(data_subject)

gold_table = metadata["gold_table"]
group_columns = metadata["group_columns"]
metric_columns_str = metadata["metric_columns"]
num_partitions = metadata["num_partitions"] or DEFAULT_NUM_PARTITIONS

# Aggregated audit table in Gold schema
agg_audit_table = f"{GOLD_SCHEMA}.{data_subject}_agg_audit"

batch_id = get_batch_id(f"agg_audit_{data_subject}")
load_job_nr = get_timestamp("%Y%m%d%H%M%S")
app_id = spark.sparkContext.applicationId

print(f"Source: {gold_table}")
print(f"Agg audit table: {agg_audit_table}")
print(f"Group columns: {group_columns}")
print(f"Metric columns: {metric_columns_str}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Today's Gold Data

# COMMAND ----------

gold_df = spark.table(gold_table)

# Filter for today's data only (current batch)
today_df = gold_df.filter(F.to_date(F.col("_ingestion_ts")) == F.current_date())
src_count = today_df.count()

print(f"Today's Gold records: {src_count}")

if src_count == 0:
    print("No data loaded today. Exiting.")
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Aggregations

# COMMAND ----------

# Parse metric columns
metric_cols = [c.strip() for c in metric_columns_str.split(",") if c.strip()]

# Filter to only valid columns that exist in the dataframe
valid_metrics = [c for c in metric_cols if c in today_df.columns]
print(f"Valid metric columns: {valid_metrics}")

# COMMAND ----------

if not valid_metrics:
    # No valid KPI columns - just count records (fallback logic)
    print("No valid KPI columns. Computing record count only.")
    
    agg_df = (
        today_df
        .groupBy(group_columns)
        .agg(
            F.count(F.lit(1)).alias("no_of_records"),
            F.first("_ingestion_ts").alias("load_date")
        )
        .withColumn("keyfigure", F.lit(""))
        .withColumn("total_qty_amount", F.lit(None).cast("double"))
        .withColumn("data_subject", F.lit(data_subject))
        .withColumn("load_date", F.current_timestamp())
        .withColumnRenamed("_file_name", "file_name")
        .withColumn("ins_gmt_ts", F.current_timestamp())
        .withColumn("ld_jb_nr", F.lit(load_job_nr))
    )
else:
    # Aggregate with metric sums
    count_expr = F.count(F.lit(1)).cast("long").alias("no_of_records")
    metric_agg_exprs = [F.sum(F.col(c)).cast("double").alias(c) for c in valid_metrics]
    timestamp_agg = F.first("_ingestion_ts").alias("load_date")
    
    all_agg_exprs = [count_expr] + metric_agg_exprs + [timestamp_agg]
    
    # Perform aggregation
    agg_df = today_df.groupBy(group_columns).agg(*all_agg_exprs)
    
    # Transpose using stack
    stack_values = ", ".join([f"'{c}', `{c}`" for c in valid_metrics])
    stack_expr = f"stack({len(valid_metrics)}, {stack_values}) as (keyfigure, total_qty_amount)"
    
    transposed_df = (
        agg_df
        .selectExpr(group_columns, "no_of_records", stack_expr, "load_date")
        .withColumn("data_subject", F.lit(data_subject))
        .withColumnRenamed("_file_name", "file_name")
        .withColumn("ins_gmt_ts", F.current_timestamp())
        .withColumn("ld_jb_nr", F.lit(load_job_nr))
    )
    
    agg_df = transposed_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Aggregated Audit Table

# COMMAND ----------

(
    agg_df
    .repartition(1)
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(agg_audit_table)
)

tgt_count = agg_df.count()
print(f"Aggregated audit table loaded: {agg_audit_table}, rows: {tgt_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Audit Entry

# COMMAND ----------

from utilities.audit_helper import write_audit_entry

write_audit_entry(
    spark=spark,
    jdbc_url=AUDIT_JDBC_URL,
    jdbc_properties=AUDIT_JDBC_PROPERTIES,
    batch_id=batch_id,
    application_id=app_id,
    object_name=agg_audit_table,
    data_layer="AGGR_AUDIT",
    job_status="SUCCESS",
    source_row_count=src_count,
    target_row_count=tgt_count,
    error_record_count=0,
    source_system=metadata["source_system"],
    file_name=gold_table,
    load_job_number=load_job_nr
)

print("Audit entry written for Aggregated Audit layer")

# COMMAND ----------

dbutils.notebook.exit(f'{{"batch_id": "{batch_id}", "tgt_count": {tgt_count}}}')
