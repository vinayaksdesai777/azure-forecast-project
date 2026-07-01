# Databricks notebook source
# MAGIC %md
# MAGIC # Audit Helper Utility
# MAGIC Writes job tracking entries to hpe_catalog.audit.job_log (Delta, Unity Catalog).
# MAGIC Replaces the previous Azure SQL JDBC write.

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
    # legacy params kept for backward compat — ignored
    jdbc_url: str = None,
    jdbc_properties: dict = None,
    application_id: str = None,
    source_row_count: int = None,
    target_row_count: int = None,
    error_record_count: int = None,
    file_name: str = None,
    load_job_number: str = None,
    job_start_ts: str = None,
    job_end_ts: str = None
):
    """
    Write one row to hpe_catalog.audit.job_log (Delta table, Unity Catalog).

    Parameters:
        batch_id         : UUID batch ID linking all layers in one run
        layer            : bronze / silver / gold / agg_audit
        status           : SUCCESS / FAILED
        records_inserted : new rows written to target
        records_updated  : rows updated via SCD2 merge
        error_records    : rows quarantined or flagged
        source_system    : SAP_HANA / SQL_SERVER / SALESFORCE
        data_subject     : o9_forecast_daily etc.
        object_name      : target Delta table name
        error_message    : exception text on failure
        run_id           : ADF pipeline run ID
    """
    # support legacy callers that pass source_row_count / error_record_count
    if records_inserted == 0 and target_row_count is not None:
        records_inserted = target_row_count
    if error_records == 0 and error_record_count is not None:
        error_records = error_record_count

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
        "object_name":      object_name or file_name,
        "error_message":    error_message,
        "run_id":           run_id or application_id,
    }

    (
        spark.createDataFrame([row])
        .withColumn("insert_time", F.to_timestamp("insert_time"))
        .write
        .format("delta")
        .mode("append")
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
    # legacy params
    jdbc_url: str = None,
    jdbc_properties: dict = None,
    application_id: str = None
):
    """Write a FAILED audit entry. Call from exception handlers."""
    write_audit_entry(
        spark=spark,
        batch_id=batch_id,
        layer=layer,
        status="FAILED",
        object_name=object_name,
        error_message=error_message,
        source_system=source_system,
        data_subject=data_subject,
        run_id=run_id or application_id,
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
    data_subject: str = None
):
    """
    Write one DQ check result to hpe_catalog.audit.data_quality_log.

    check_type values: NULL_PK / DEDUP / CURRENCY_VALIDATION / CATEGORY_VALIDATION / TYPE_CAST
    """
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
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("hpe_catalog.audit.data_quality_log")
    )

    print(f"  [DQ] {check_type.upper()} | {table_name} | "
          f"checked={records_checked} failed={records_failed}")
