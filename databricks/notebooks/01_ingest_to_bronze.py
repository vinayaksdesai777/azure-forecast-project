# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingest to Bronze
# MAGIC Orchestrates: read Parquet → add audit cols → standardize → DQ checks → write Delta.
# MAGIC All logic lives in utilities/ (Strategy pattern). This notebook is an orchestrator only.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from utilities.audit_helper    import write_audit_entry, mark_audit_failed
from utilities.dq_checks       import NullPKCheck, DedupCheck, run_dq_checks
from utilities.transformations import AddAuditColumnsTransform, StandardizeTransform, apply_transforms

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

source_path = source_path_ovr if source_path_ovr else get_adls_path(CONTAINER_LANDING, metadata["landing_path"])
batch_id    = get_batch_id(f"bronze_{data_subject}")
load_job_nr = get_timestamp()

print(f"data_subject : {data_subject} | batch_id : {batch_id}")

# COMMAND ----------

# MAGIC %md ## Read Parquet from Landing

# COMMAND ----------

try:
    raw_df = spark.read.format("parquet").load(source_path)
except Exception as e:
    mark_audit_failed(spark, batch_id=batch_id, layer="bronze", object_name=bronze_table,
                      error_message=str(e), source_system=source_system,
                      data_subject=data_subject, run_id=run_id)
    raise

src_count = raw_df.count()
print(f"Landing records: {src_count}")

if src_count == 0:
    write_audit_entry(spark, batch_id=batch_id, layer="bronze", status="SUCCESS",
                      records_inserted=0, source_system=source_system,
                      data_subject=data_subject, object_name=bronze_table, run_id=run_id)
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# MAGIC %md ## Apply Transformations (Strategy Pipeline)

# COMMAND ----------

enriched_df = apply_transforms(raw_df, [
    AddAuditColumnsTransform(frequency=frequency, load_job_nr=load_job_nr, batch_id=batch_id),
    StandardizeTransform(upper_cols=["category"]),
])

# COMMAND ----------

# MAGIC %md ## Run DQ Checks (Strategy Pattern)

# COMMAND ----------

clean_df, total_quarantined, dq_results = run_dq_checks(
    spark=spark,
    df=enriched_df,
    checks=[
        NullPKCheck(pk_cols=null_check_cols),
        DedupCheck(pk_cols=null_check_cols),
    ],
    batch_id=batch_id,
    layer="bronze",
    table_name=bronze_table,
    data_subject=data_subject,
    quarantine_table=f"{BRONZE_SCHEMA}.quarantine",
)

final_count = clean_df.count()
dup_count   = next((r["records_failed"] for r in dq_results if r["check_type"] == "DEDUP"), 0)
null_count  = next((r["records_failed"] for r in dq_results if r["check_type"] == "NULL_PK"), 0)
print(f"After DQ — clean: {final_count} | quarantined: {total_quarantined}")

# COMMAND ----------

# MAGIC %md ## Write to Bronze Delta

# COMMAND ----------

try:
    (
        clean_df
        .repartition(num_partitions)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("replaceWhere", f"_frequency = '{frequency}' AND date(_ingestion_ts) = current_date()")
        .saveAsTable(bronze_table)
    )
    print(f"Bronze written: {bronze_table} | rows: {spark.table(bronze_table).count()}")
except Exception as e:
    mark_audit_failed(spark, batch_id=batch_id, layer="bronze", object_name=bronze_table,
                      error_message=str(e), source_system=source_system,
                      data_subject=data_subject, run_id=run_id)
    raise

# COMMAND ----------

# MAGIC %md ## Archive + Audit

# COMMAND ----------

dbutils.fs.cp(source_path, get_adls_path(CONTAINER_ARCHIVE, f"{data_subject}/{load_job_nr}/"), recurse=True)

write_audit_entry(
    spark=spark, batch_id=batch_id, layer="bronze", status="SUCCESS",
    records_inserted=final_count, records_updated=0,
    error_records=total_quarantined,
    source_system=source_system, data_subject=data_subject,
    object_name=bronze_table, run_id=run_id
)

dbutils.notebook.exit(f'{{"batch_id":"{batch_id}","src_count":{src_count},"bronze_count":{final_count},"quarantine_count":{null_count},"dup_count":{dup_count}}}')
