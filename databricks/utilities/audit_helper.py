# Databricks notebook source
# MAGIC %md
# MAGIC # Audit Helper Utility
# MAGIC Writes job tracking and DQ entries to hpe_catalog.audit (Unity Catalog Delta).
# MAGIC No Azure SQL. No JDBC.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime


def write_audit_entry(
    spark: SparkSession,
    batch_id: str,
    layer: str,
    status: str,
    records_inserted: int = 0,
    records_updated: int = 0,
    error_records: int = 0,
    source_system: str = None,
    data_subject: str = None,
    object_name: str = None,
    error_message: str = None,
    run_id: str = None,
):
    row = {
        "batch_id":         batch_id,
        "insert_time":      datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "layer":            layer.lower(),
        "status":           status.upper(),
        "records_inserted": int(records_inserted or 0),
        "records_updated":  int(records_updated or 0),
        "error_records":    int(error_records or 0),
        "source_system":    source_system,
        "data_subject":     data_subject,
        "object_name":      object_name,
        "error_message":    error_message,
        "run_id":           run_id,
    }
    (
        spark.createDataFrame([row])
        .withColumn("insert_time", F.to_timestamp("insert_time"))
        .write.format("delta").mode("append")
        .saveAsTable("hpe_catalog.audit.job_log")
    )
    print(f"  [AUDIT] {layer.upper()} | {object_name} | {status.upper()} | "
          f"inserted={records_inserted} updated={records_updated} errors={error_records}")


def mark_audit_failed(
    spark: SparkSession,
    batch_id: str,
    layer: str,
    object_name: str = None,
    error_message: str = None,
    source_system: str = None,
    data_subject: str = None,
    run_id: str = None,
):
    write_audit_entry(
        spark=spark, batch_id=batch_id, layer=layer, status="FAILED",
        object_name=object_name, error_message=error_message,
        source_system=source_system, data_subject=data_subject, run_id=run_id,
    )


def log_dq_result(
    spark: SparkSession,
    batch_id: str,
    layer: str,
    table_name: str,
    check_type: str,
    column_name: str,
    records_checked: int,
    records_failed: int,
    data_subject: str = None,
):
    row = {
        "batch_id":        batch_id,
        "insert_time":     datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "layer":           layer.lower(),
        "table_name":      table_name,
        "check_type":      check_type.upper(),
        "column_name":     column_name,
        "records_checked": int(records_checked or 0),
        "records_passed":  int((records_checked or 0) - (records_failed or 0)),
        "records_failed":  int(records_failed or 0),
        "data_subject":    data_subject,
    }
    (
        spark.createDataFrame([row])
        .withColumn("insert_time", F.to_timestamp("insert_time"))
        .write.format("delta").mode("append")
        .saveAsTable("hpe_catalog.audit.data_quality_log")
    )
    print(f"  [DQ] {check_type.upper()} | {table_name} | checked={records_checked} failed={records_failed}")
