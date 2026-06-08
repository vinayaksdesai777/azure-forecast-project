# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze to Silver (Cleansed/Reference Layer)
# MAGIC 
# MAGIC Applies data quality checks and loads validated data to Silver Delta table.
# MAGIC Performs PK null checks, type casting, and empty string cleansing.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType
from delta.tables import DeltaTable

# COMMAND ----------

# Widget parameters (passed from ADF or upstream notebook)
dbutils.widgets.text("data_subject", "", "Data Subject")
dbutils.widgets.text("batch_id", "", "Batch ID from Bronze load")

data_subject = dbutils.widgets.get("data_subject")
upstream_batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

# Load metadata
metadata = get_pipeline_metadata(data_subject)

bronze_table = metadata["bronze_table"]
silver_table = metadata["silver_table"]
null_check_columns = metadata["null_check_columns"]
delimiter = metadata["delimiter"] or DEFAULT_DELIMITER
num_partitions = metadata["num_partitions"] or DEFAULT_NUM_PARTITIONS

batch_id = get_batch_id(f"silver_{data_subject}")
load_job_nr = get_timestamp("%Y%m%d%H%M%S")
app_id = spark.sparkContext.applicationId

print(f"Source: {bronze_table}")
print(f"Target: {silver_table}")
print(f"Null check columns: {null_check_columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

bronze_df = spark.table(bronze_table)
src_count = bronze_df.count()
print(f"Bronze records: {src_count}")

if src_count == 0:
    print("No data in bronze table. Exiting.")
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality - Null Check on Primary Key Columns

# COMMAND ----------

from utilities.data_quality import validate_not_null, log_dq_results

valid_df = bronze_df
error_count = 0

if null_check_columns:
    pk_columns = [col.strip() for col in null_check_columns.split(",")]
    
    # Filter: keep only rows where all PK columns are non-null and non-empty
    null_condition = F.lit(True)
    for col_name in pk_columns:
        if col_name in bronze_df.columns:
            null_condition = null_condition & (
                F.col(col_name).isNotNull() & 
                (F.trim(F.col(col_name)) != "")
            )
    
    valid_df = bronze_df.filter(null_condition)
    invalid_df = bronze_df.filter(~null_condition)
    
    error_count = invalid_df.count()
    valid_count = valid_df.count()
    
    print(f"Valid records: {valid_count}")
    print(f"Invalid records (null PKs): {error_count}")
    
    # Log DQ results to Azure SQL
    log_dq_results(
        spark=spark,
        jdbc_url=AUDIT_JDBC_URL,
        jdbc_properties=AUDIT_JDBC_PROPERTIES,
        batch_id=batch_id,
        table_name=silver_table,
        check_type="NULL_CHECK",
        column_name=null_check_columns,
        records_checked=src_count,
        records_failed=error_count
    )
else:
    valid_count = src_count
    print("No null check columns configured. Passing all records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanse Data - Replace Empty Strings with Null

# COMMAND ----------

# Replace empty strings with null for cleaner downstream analytics
for col_name in valid_df.columns:
    if valid_df.schema[col_name].dataType == StringType():
        valid_df = valid_df.withColumn(
            col_name,
            F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.col(col_name))
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Delta Table

# COMMAND ----------

# Add silver-specific columns
silver_df = (
    valid_df
    .withColumn("_ingestion_date", F.current_date())
    .withColumn("_silver_load_ts", F.current_timestamp())
    .withColumn("_batch_id", F.lit(batch_id))
)

# Drop internal bronze columns not needed in silver
cols_to_drop = ["_source_batch_nr", "_load_job_nr"]
existing_drop_cols = [c for c in cols_to_drop if c in silver_df.columns]
if existing_drop_cols:
    silver_df = silver_df.drop(*existing_drop_cols)

# Write to Silver (append mode for historical accumulation)
(
    silver_df
    .repartition(num_partitions)
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(silver_table)
)

tgt_count = valid_count
print(f"Silver table loaded: {silver_table}, rows: {tgt_count}")

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
    object_name=silver_table,
    data_layer="SILVER",
    job_status="SUCCESS",
    source_row_count=src_count,
    target_row_count=tgt_count,
    error_record_count=error_count,
    source_system=metadata["source_system"],
    file_name=bronze_table,
    load_job_number=load_job_nr
)

print("Audit entry written for Silver layer")

# COMMAND ----------

dbutils.notebook.exit(f'{{"batch_id": "{batch_id}", "valid_count": {valid_count}, "error_count": {error_count}}}')
