# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingest to Bronze (Raw Layer)
# MAGIC 
# MAGIC Reads CSV files from ADLS Landing zone and writes to Bronze Delta table.
# MAGIC Applies schema-on-read (all STRING) with audit column enrichment.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from delta.tables import DeltaTable
import sys

# COMMAND ----------

# Widget parameters (passed from ADF)
dbutils.widgets.text("data_subject", "", "Data Subject")
dbutils.widgets.text("source_path", "", "Source File Path")

data_subject = dbutils.widgets.get("data_subject")
source_path_override = dbutils.widgets.get("source_path")

# COMMAND ----------

# Load metadata configuration
metadata = get_pipeline_metadata(data_subject)

source_system = metadata["source_system"]
frequency = metadata["frequency"]
delimiter = metadata["delimiter"] or DEFAULT_DELIMITER
has_header = metadata["has_header"]
bronze_table = metadata["bronze_table"]
num_partitions = metadata["num_partitions"] or DEFAULT_NUM_PARTITIONS

# Determine source path
source_path = source_path_override if source_path_override else get_adls_path(CONTAINER_LANDING, metadata["source_path"])

print(f"Processing data_subject: {data_subject}")
print(f"Frequency: {frequency}")
print(f"Source path: {source_path}")
print(f"Target bronze table: {bronze_table}")

# COMMAND ----------

# Generate batch identifiers
batch_id = get_batch_id(f"bronze_{data_subject}")
load_timestamp = get_timestamp("%Y-%m-%d %H:%M:%S")
load_job_nr = get_timestamp("%Y%m%d%H%M%S")
app_id = spark.sparkContext.applicationId

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Source CSV Files

# COMMAND ----------

# Read source files from landing zone
file_df = (
    spark.read
    .format("csv")
    .option("header", str(has_header).lower())
    .option("delimiter", delimiter)
    .option("inferSchema", "false")  # All as string for bronze
    .load(source_path)
)

# Add operational/audit columns
bronze_df = (
    file_df
    .withColumn("_file_name", F.element_at(F.split(F.input_file_name(), "/"), -1))
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_update_ts", F.current_timestamp())
    .withColumn("_source_batch_nr", 
                F.regexp_extract(F.col("_file_name"), r".*_(\d{12})\.csv$", 1))
    .withColumn("_frequency", F.lit(frequency))
    .withColumn("_load_job_nr", F.lit(load_job_nr))
    .withColumn("_batch_id", F.lit(batch_id))
)

# COMMAND ----------

# Get source count
src_count = bronze_df.count()
print(f"Source record count: {src_count}")

if src_count == 0:
    print(f"WARNING: No records found in source path: {source_path}")
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta Table

# COMMAND ----------

# Write to Bronze layer (overwrite for full refresh)
(
    bronze_df
    .repartition(num_partitions)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

tgt_count = spark.table(bronze_table).count()
print(f"Bronze table loaded: {bronze_table}, rows: {tgt_count}")

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
    object_name=bronze_table,
    data_layer="BRONZE",
    job_status="SUCCESS",
    source_row_count=src_count,
    target_row_count=tgt_count,
    error_record_count=0,
    source_system=source_system,
    file_name=source_path,
    load_job_number=load_job_nr
)

print("Audit entry written for Bronze layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Archive Source Files

# COMMAND ----------

# Move processed files to archive
archive_path = get_adls_path(CONTAINER_ARCHIVE, f"{data_subject}/{load_job_nr}/")
dbutils.fs.cp(source_path, archive_path, recurse=True)
print(f"Source files archived to: {archive_path}")

# COMMAND ----------

# Return metadata for downstream notebooks
dbutils.notebook.exit(f'{{"batch_id": "{batch_id}", "src_count": {src_count}, "bronze_table": "{bronze_table}"}}')
