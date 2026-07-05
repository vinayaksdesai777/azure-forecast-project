# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Config
# MAGIC Shared configuration for all pipeline notebooks.
# MAGIC No Azure SQL. No JDBC. Everything lives in Unity Catalog.

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

# ADLS containers — storage account name from Key Vault, no hard-coded creds
STORAGE_ACCOUNT   = dbutils.secrets.get(scope="kv-scope", key="adls-account-name")
CONTAINER_LANDING = "landing"
CONTAINER_ARCHIVE = "archive"

def get_adls_path(container: str, path: str = "") -> str:
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"

# COMMAND ----------

# Unity Catalog audit tables — job tracking + DQ + pipeline config
JOB_LOG_TABLE    = f"{AUDIT_SCHEMA}.job_log"
DQ_LOG_TABLE     = f"{AUDIT_SCHEMA}.data_quality_log"
PIPELINE_CONFIG  = f"{AUDIT_SCHEMA}.pipeline_config"

# COMMAND ----------

# Pipeline defaults
DEFAULT_NUM_PARTITIONS = 8

# COMMAND ----------

def get_batch_id(prefix: str = "") -> str:
    uid = str(uuid.uuid4()).replace("-", "")[:16]
    return f"{prefix}_{uid}" if prefix else uid

def get_timestamp(fmt: str = "%Y%m%d%H%M%S") -> str:
    return datetime.utcnow().strftime(fmt)

# COMMAND ----------

def get_pipeline_metadata(data_subject: str) -> dict:
    """
    Read runtime config for a data_subject from hpe_catalog.audit.pipeline_config (Delta).
    No JDBC. No Azure SQL.
    """
    df = spark.sql(f"""
        SELECT *
        FROM   {PIPELINE_CONFIG}
        WHERE  data_subject = '{data_subject}'
          AND  is_active    = true
    """)
    if df.count() == 0:
        raise ValueError(f"No active pipeline_config row for: {data_subject}")
    return df.first().asDict()

# COMMAND ----------

# Valid domain values used for Silver business logic validation
VALID_CATEGORIES = {"SERVER", "STORAGE", "COMPUTE", "NETWORKING", "PRIVATE_CLOUD", "SUPERCOMPUTING", "AI"}
VALID_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "INR", "SGD", "AED", "BRL", "CAD", "AUD"}

print(f"Config loaded — catalog: {CATALOG} | storage: {STORAGE_ACCOUNT}")
print(f"No Azure SQL dependency. All config from Unity Catalog: {PIPELINE_CONFIG}")
