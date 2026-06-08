# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration Notebook
# MAGIC Centralized configuration for the Azure Data Pipeline.
# MAGIC Stores storage, database, and pipeline default settings.

# COMMAND ----------

# Storage Configuration
STORAGE_ACCOUNT = "your_adls_storage_account"
CONTAINER_LANDING = "landing"
CONTAINER_BRONZE = "bronze"
CONTAINER_SILVER = "silver"
CONTAINER_GOLD = "gold"
CONTAINER_ARCHIVE = "archive"

ADLS_BASE_PATH = f"abfss://{{container}}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

def get_adls_path(container: str, path: str = "") -> str:
    """Get the full ADLS path for a given container and relative path."""
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"

# COMMAND ----------

# Database Configuration (Delta Lake / Unity Catalog)
CATALOG = "hpe_catalog"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

# COMMAND ----------

# Azure SQL Audit Database Configuration
AUDIT_JDBC_URL = "jdbc:sqlserver://your-sql-server.database.windows.net:1433;database=audit_db"
AUDIT_JDBC_PROPERTIES = {
    "user": dbutils.secrets.get(scope="kv-scope", key="sql-user"),
    "password": dbutils.secrets.get(scope="kv-scope", key="sql-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
AUDIT_TABLE = "audit.pipeline_audit"
DQ_LOG_TABLE = "audit.data_quality_log"

# COMMAND ----------

# Pipeline Defaults
DEFAULT_FILE_FORMAT = "csv"
DEFAULT_DELIMITER = "|"
DEFAULT_NUM_PARTITIONS = 8

# COMMAND ----------

# Helper to read pipeline metadata from Azure SQL
def get_pipeline_metadata(data_subject: str) -> dict:
    """Fetch pipeline metadata for a data subject from Azure SQL."""
    metadata_df = (
        spark.read
        .jdbc(
            url=AUDIT_JDBC_URL,
            table="audit.pipeline_metadata",
            properties=AUDIT_JDBC_PROPERTIES
        )
        .filter(f"data_subject = '{data_subject}' AND is_active = 1")
    )
    
    if metadata_df.count() == 0:
        raise ValueError(f"No active metadata found for data_subject: {data_subject}")
    
    row = metadata_df.first()
    return row.asDict()

# COMMAND ----------

# Timestamp utilities
from datetime import datetime

def get_timestamp(fmt: str = "%Y%m%d%H%M%S") -> str:
    """Get current timestamp in the specified format."""
    return datetime.utcnow().strftime(fmt)

def get_batch_id(job_name: str) -> str:
    """Generate a unique batch ID."""
    return f"{job_name}_{get_timestamp()}"
