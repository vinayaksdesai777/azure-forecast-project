# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver to Gold (Dimension/Business Layer)
# MAGIC 
# MAGIC Transforms Silver data into business-ready Gold Delta tables.
# MAGIC Derives period column and partitions by period and frequency.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("data_subject", "", "Data Subject")
dbutils.widgets.text("batch_id", "", "Batch ID from Silver load")

data_subject = dbutils.widgets.get("data_subject")
upstream_batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

# Load metadata
metadata = get_pipeline_metadata(data_subject)

silver_table = metadata["silver_table"]
gold_table = metadata["gold_table"]
partition_column = metadata["partition_column"]
apply_concat = metadata["apply_concat"]
num_partitions = metadata["num_partitions"] or DEFAULT_NUM_PARTITIONS

batch_id = get_batch_id(f"gold_{data_subject}")
load_job_nr = get_timestamp("%Y%m%d%H%M%S")
app_id = spark.sparkContext.applicationId

print(f"Source: {silver_table}")
print(f"Target: {gold_table}")
print(f"Partition column: {partition_column}")
print(f"Apply concat for period: {apply_concat}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Data

# COMMAND ----------

silver_df = spark.table(silver_table)
src_count = silver_df.count()
print(f"Silver records: {src_count}")

if src_count == 0:
    print("No data in silver table. Exiting.")
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Period Partition Column

# COMMAND ----------

# Create the period column for partitioning
if partition_column and partition_column in silver_df.columns:
    if apply_concat:
        # concat_ws("-", substring(col(partitionColumn), 1, 7), lit("01"))
        # Produces: "YYYY-MM-01" from a date column
        gold_df = silver_df.withColumn(
            "period",
            F.concat_ws("-", F.substring(F.col(partition_column), 1, 7), F.lit("01"))
        )
    else:
        gold_df = silver_df.withColumn("period", F.col(partition_column))
else:
    gold_df = silver_df
    print(f"Warning: partition_column '{partition_column}' not found. Skipping period derivation.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Delta Table

# COMMAND ----------

# Add gold-specific metadata
gold_df = (
    gold_df
    .withColumn("_gold_load_ts", F.current_timestamp())
    .withColumn("_batch_id", F.lit(batch_id))
)

# Write to Gold layer (overwrite for full refresh dimension load)
(
    gold_df
    .repartition(num_partitions)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("period") if partition_column else gold_df
    .repartition(num_partitions)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

# Proper write with conditional partition
if partition_column and partition_column in silver_df.columns:
    (
        gold_df
        .repartition(num_partitions)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("period", "_frequency")
        .saveAsTable(gold_table)
    )
else:
    (
        gold_df
        .repartition(num_partitions)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(gold_table)
    )

tgt_count = spark.table(gold_table).count()
print(f"Gold table loaded: {gold_table}, rows: {tgt_count}")

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
    object_name=gold_table,
    data_layer="GOLD",
    job_status="SUCCESS",
    source_row_count=src_count,
    target_row_count=tgt_count,
    error_record_count=0,
    source_system=metadata["source_system"],
    file_name=silver_table,
    load_job_number=load_job_nr
)

print("Audit entry written for Gold layer")

# COMMAND ----------

dbutils.notebook.exit(f'{{"batch_id": "{batch_id}", "tgt_count": {tgt_count}, "gold_table": "{gold_table}"}}')
