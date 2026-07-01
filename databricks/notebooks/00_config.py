# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Config
# MAGIC Shared configuration for all pipeline notebooks.
# MAGIC Sets catalog, storage paths, JDBC for pipeline config reads, and helper functions.

# COMMAND ----------

import uuid
from datetime import datetime

# COMMAND ----------

# Unity Catalog
CATALOG        = "hpe_catalog"
BRONZE_SCHEMA  = f"{CATALOG}.bronze"
SILVER_SCHEMA  = f"{CATALOG}.silver"
GOLD_SCHEMA    = f"{CATALOG}.gold"
AUDIT_SCHEMA   = f"{CATALOG}.audit"

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# ADLS containers
STORAGE_ACCOUNT   = dbutils.secrets.get(scope="kv-scope", key="adls-account-name")
CONTAINER_LANDING = "landing"
CONTAINER_ARCHIVE = "archive"

def get_adls_path(container: str, path: str = "") -> str:
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"

# COMMAND ----------

# Azure SQL — pipeline config only (not job tracking)
_SQL_SERVER   = dbutils.secrets.get(scope="kv-scope", key="sql-server-fqdn")
_SQL_USER     = dbutils.secrets.get(scope="kv-scope", key="sql-user")
_SQL_PASSWORD = dbutils.secrets.get(scope="kv-scope", key="sql-password")

PIPELINE_JDBC_URL = f"jdbc:sqlserver://{_SQL_SERVER}:1433;database=audit-db;encrypt=true;trustServerCertificate=false"
PIPELINE_JDBC_PROPS = {
    "user":     _SQL_USER,
    "password": _SQL_PASSWORD,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# Unity Catalog audit tables (job tracking + DQ — NOT Azure SQL)
JOB_LOG_TABLE = f"{AUDIT_SCHEMA}.job_log"
DQ_LOG_TABLE  = f"{AUDIT_SCHEMA}.data_quality_log"

# COMMAND ----------

# Pipeline defaults
DEFAULT_DELIMITER      = "|"
DEFAULT_NUM_PARTITIONS = 8

# COMMAND ----------

def get_batch_id(prefix: str = "") -> str:
    uid = str(uuid.uuid4()).replace("-", "")[:16]
    return f"{prefix}_{uid}" if prefix else uid

def get_timestamp(fmt: str = "%Y%m%d%H%M%S") -> str:
    return datetime.utcnow().strftime(fmt)

# COMMAND ----------

def get_pipeline_metadata(data_subject: str) -> dict:
    """Read runtime config for a data_subject from Azure SQL pipeline_metadata."""
    df = (
        spark.read
        .jdbc(url=PIPELINE_JDBC_URL, table="audit.pipeline_metadata", properties=PIPELINE_JDBC_PROPS)
        .filter(f"data_subject = '{data_subject}' AND is_active = 1")
    )
    if df.count() == 0:
        raise ValueError(f"No active pipeline metadata for: {data_subject}")
    return df.first().asDict()

# COMMAND ----------

# Valid domain values used for Silver business logic validation
VALID_CATEGORIES = {"SERVER", "STORAGE", "COMPUTE", "NETWORKING", "PRIVATE_CLOUD", "SUPERCOMPUTING", "AI"}
VALID_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "INR", "SGD", "AED", "BRL", "CAD", "AUD"}

print(f"Config loaded — catalog: {CATALOG}, storage: {STORAGE_ACCOUNT}")
