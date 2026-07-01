# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingest to Bronze
# MAGIC Reads Parquet from landing, standardizes columns, runs PK null check,
# MAGIC deduplicates, routes bad rows to quarantine, saves clean rows to Bronze Delta.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from utilities.audit_helper import write_audit_entry, mark_audit_failed, log_dq_result

# COMMAND ----------

dbutils.widgets.text("data_subject", "", "Data Subject")
dbutils.widgets.text("source_path",  "", "Source Path Override")
dbutils.widgets.text("run_id",       "", "ADF Pipeline Run ID")

data_subject    = dbutils.widgets.get("data_subject")
source_path_ovr = dbutils.widgets.get("source_path")
run_id          = dbutils.widgets.get("run_id")

# COMMAND ----------

metadata        = get_pipeline_metadata(data_subject)
source_system   = metadata["source_system"]
frequency       = metadata["frequency"]
bronze_table    = metadata["bronze_table"]
null_check_cols = [c.strip() for c in metadata.get("null_check_columns", "product_id,location_id,forecast_date").split(",")]
num_partitions  = int(metadata.get("num_partitions") or DEFAULT_NUM_PARTITIONS)

source_path = source_path_ovr if source_path_ovr else get_adls_path(CONTAINER_LANDING, metadata["source_path"])

batch_id    = get_batch_id(f"bronze_{data_subject}")
load_job_nr = get_timestamp()

print(f"data_subject : {data_subject}")
print(f"source_path  : {source_path}")
print(f"bronze_table : {bronze_table}")
print(f"batch_id     : {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Parquet from Landing

# COMMAND ----------

try:
    raw_df = spark.read.format("parquet").load(source_path)
except Exception as e:
    mark_audit_failed(spark, batch_id=batch_id, layer="bronze", object_name=bronze_table,
                      error_message=str(e), source_system=source_system,
                      data_subject=data_subject, run_id=run_id)
    raise

src_count = raw_df.count()
print(f"Records read from landing: {src_count}")

if src_count == 0:
    write_audit_entry(spark, batch_id=batch_id, layer="bronze", status="SUCCESS",
                      records_inserted=0, source_system=source_system,
                      data_subject=data_subject, object_name=bronze_table, run_id=run_id)
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Audit Columns + Standardize

# COMMAND ----------

enriched_df = (
    raw_df
    .withColumn("_file_name",    F.element_at(F.split(F.input_file_name(), "/"), -1))
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_update_ts",    F.current_timestamp())
    .withColumn("_frequency",    F.lit(frequency))
    .withColumn("_load_job_nr",  F.lit(load_job_nr))
    .withColumn("_batch_id",     F.lit(batch_id))
)

# TRIM all source string columns + UPPER(category)
string_cols = [f.name for f in enriched_df.schema.fields
               if str(f.dataType) == "StringType()" and not f.name.startswith("_")]

for col in string_cols:
    enriched_df = enriched_df.withColumn(col, F.trim(F.col(col)))

if "category" in string_cols:
    enriched_df = enriched_df.withColumn("category", F.upper(F.col("category")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## PK Null Check → Quarantine

# COMMAND ----------

null_condition = F.lit(False)
for col in null_check_cols:
    if col in enriched_df.columns:
        null_condition = null_condition | F.col(col).isNull() | (F.trim(F.col(col)) == "")

valid_df   = enriched_df.filter(~null_condition)
invalid_df = enriched_df.filter(null_condition).withColumn("_dq_fail_reason", F.lit("NULL_PK"))

null_fail_count  = invalid_df.count()
null_valid_count = valid_df.count()
print(f"PK null check — valid: {null_valid_count} | quarantined: {null_fail_count}")

if null_fail_count > 0:
    (
        invalid_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"{BRONZE_SCHEMA}.quarantine")
    )

log_dq_result(spark, batch_id=batch_id, layer="bronze",
              table_name=bronze_table, check_type="NULL_PK",
              column_name=",".join(null_check_cols),
              records_checked=src_count, records_failed=null_fail_count,
              data_subject=data_subject)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication

# COMMAND ----------

pk_window  = Window.partitionBy(*null_check_cols, "_frequency").orderBy(F.col("_ingestion_ts").desc())
deduped_df = valid_df.withColumn("_rn", F.row_number().over(pk_window)).filter(F.col("_rn") == 1).drop("_rn")
dup_count  = null_valid_count - deduped_df.count()
final_count = deduped_df.count()
print(f"Dedup — dropped: {dup_count} | remaining: {final_count}")

log_dq_result(spark, batch_id=batch_id, layer="bronze",
              table_name=bronze_table, check_type="DEDUP",
              column_name=",".join(null_check_cols),
              records_checked=null_valid_count, records_failed=dup_count,
              data_subject=data_subject)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta

# COMMAND ----------

try:
    (
        deduped_df
        .repartition(num_partitions)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("replaceWhere", f"_frequency = '{frequency}' AND date(_ingestion_ts) = current_date()")
        .saveAsTable(bronze_table)
    )
    tgt_count = spark.table(bronze_table).count()
    print(f"Bronze written: {bronze_table} | rows: {tgt_count}")
except Exception as e:
    mark_audit_failed(spark, batch_id=batch_id, layer="bronze", object_name=bronze_table,
                      error_message=str(e), source_system=source_system,
                      data_subject=data_subject, run_id=run_id)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Archive Source + Write Audit

# COMMAND ----------

archive_path = get_adls_path(CONTAINER_ARCHIVE, f"{data_subject}/{load_job_nr}/")
dbutils.fs.cp(source_path, archive_path, recurse=True)
print(f"Archived to: {archive_path}")

write_audit_entry(
    spark=spark, batch_id=batch_id, layer="bronze", status="SUCCESS",
    records_inserted=final_count, records_updated=0,
    error_records=null_fail_count + dup_count,
    source_system=source_system, data_subject=data_subject,
    object_name=bronze_table, run_id=run_id
)

# COMMAND ----------

dbutils.notebook.exit(f'{{"batch_id":"{batch_id}","src_count":{src_count},"bronze_count":{final_count},"quarantine_count":{null_fail_count},"dup_count":{dup_count}}}')
