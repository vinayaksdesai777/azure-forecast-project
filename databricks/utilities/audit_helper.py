# Databricks notebook source
# MAGIC %md
# MAGIC # Audit Helper Utility
# MAGIC Provides functions to write audit entries to Azure SQL Database.

# COMMAND ----------

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from datetime import datetime


def write_audit_entry(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_properties: dict,
    batch_id: str,
    application_id: str,
    object_name: str,
    data_layer: str,
    job_status: str,
    source_row_count: int = 0,
    target_row_count: int = 0,
    error_record_count: int = 0,
    source_system: str = None,
    file_name: str = None,
    load_job_number: str = None,
    job_start_ts: str = None,
    job_end_ts: str = None
):
    """
    Write an audit entry to the Azure SQL audit.pipeline_audit table.
    
    Parameters:
        spark: SparkSession instance
        jdbc_url: JDBC connection URL for Azure SQL
        jdbc_properties: Dict with user, password, driver
        batch_id: Unique batch identifier
        application_id: Spark application ID
        object_name: Target table name
        data_layer: Layer name (BRONZE, SILVER, GOLD, AGGR_AUDIT)
        job_status: Status (SUCCESS, FAILED, RUNNING)
        source_row_count: Number of source records
        target_row_count: Number of target records
        error_record_count: Number of error records
        source_system: Source system name
        file_name: Source file or table name
        load_job_number: Load job identifier
        job_start_ts: Job start timestamp (auto-generated if None)
        job_end_ts: Job end timestamp (auto-generated if None)
    """
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    start_ts = job_start_ts or now
    end_ts = job_end_ts or now
    
    # Calculate duration
    start_dt = datetime.strptime(start_ts, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end_ts, "%Y-%m-%d %H:%M:%S")
    duration_sec = int((end_dt - start_dt).total_seconds())
    
    audit_row = Row(
        batch_id=batch_id,
        application_id=application_id,
        object_name=object_name,
        data_layer=data_layer,
        job_start_ts=start_ts,
        job_end_ts=end_ts,
        job_status=job_status,
        source_row_count=source_row_count,
        target_row_count=target_row_count,
        error_record_count=error_record_count,
        job_duration_sec=duration_sec,
        source_system=source_system,
        file_name=file_name,
        load_job_number=load_job_number,
        created_by=application_id
    )
    
    audit_df = spark.createDataFrame([audit_row])
    
    (
        audit_df.write
        .jdbc(
            url=jdbc_url,
            table="audit.pipeline_audit",
            mode="append",
            properties=jdbc_properties
        )
    )
    
    print(f"  [AUDIT] {data_layer} | {object_name} | {job_status} | "
          f"src={source_row_count} tgt={target_row_count} err={error_record_count}")


def mark_audit_failed(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_properties: dict,
    batch_id: str,
    application_id: str,
    object_name: str,
    data_layer: str,
    error_message: str = None,
    source_system: str = None
):
    """
    Write a FAILED audit entry. Used in exception handlers.
    """
    write_audit_entry(
        spark=spark,
        jdbc_url=jdbc_url,
        jdbc_properties=jdbc_properties,
        batch_id=batch_id,
        application_id=application_id,
        object_name=object_name,
        data_layer=data_layer,
        job_status="FAILED",
        source_system=source_system
    )
    
    print(f"  [AUDIT-FAILED] {data_layer} | {object_name}")
    if error_message:
        print(f"  [ERROR] {error_message}")
