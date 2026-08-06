# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingest HANA to Landing (JDBC)
# MAGIC Connects to SAP HANA Cloud via JDBC (ngdbc.jar) and writes Parquet to ADLS landing.
# MAGIC Called by ADF in place of the Copy Activity for SapHana connector, which requires
# MAGIC the full HDB client with crypto (unavailable on tools.hana.ondemand.com downloads).
# MAGIC
# MAGIC Inputs (ADF parameters):
# MAGIC   data_subject    - e.g. hana_forecast_daily, hana_forecast_quarterly
# MAGIC   run_id          - ADF pipeline run ID for audit
# MAGIC   new_watermark   - ISO timestamp set by ADF (utcNow), written back after success

# COMMAND ----------

import uuid
from datetime import datetime

spark.sql("USE CATALOG hpe_catalog")

SECRET_SCOPE      = "hpe-forecast"
STORAGE_ACCOUNT   = dbutils.secrets.get(scope=SECRET_SCOPE, key="adls-account-name")
CONTAINER_LANDING = "landing"
PIPELINE_CONFIG   = "hpe_catalog.audit.pipeline_config"
JOB_LOG_TABLE     = "hpe_catalog.audit.job_log"

def get_adls_path(container, path=""):
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"

def get_batch_id(prefix=""):
    uid = str(uuid.uuid4()).replace("-", "")[:16]
    return f"{prefix}_{uid}" if prefix else uid

def get_timestamp(fmt="%Y%m%d%H%M%S"):
    return datetime.utcnow().strftime(fmt)

# COMMAND ----------

dbutils.widgets.text("data_subject",  "", "Data Subject")
dbutils.widgets.text("run_id",        "", "ADF Pipeline Run ID")
dbutils.widgets.text("new_watermark", "", "New Watermark (utcNow from ADF)")

data_subject  = dbutils.widgets.get("data_subject").strip()
run_id        = dbutils.widgets.get("run_id").strip()
new_watermark = dbutils.widgets.get("new_watermark").strip()

if not data_subject:
    raise ValueError("data_subject widget is required")

# COMMAND ----------

row = spark.sql(f"""
    SELECT source_system, source_schema, source_object,
           landing_path, load_type, watermark_column, last_watermark
    FROM   {PIPELINE_CONFIG}
    WHERE  data_subject = '{data_subject}' AND is_active = true
""").first()

if not row:
    raise ValueError(f"No active pipeline_config for data_subject='{data_subject}'")

source_schema    = row["source_schema"]
source_object    = row["source_object"]
landing_path_rel = row["landing_path"]
load_type        = row["load_type"]
watermark_col    = row["watermark_column"] or ""
last_watermark   = row["last_watermark"] or ""
source_system    = row["source_system"]

landing_path = get_adls_path(CONTAINER_LANDING, landing_path_rel)
batch_id     = get_batch_id(f"hana_{data_subject}")
load_job_nr  = get_timestamp()

print(f"data_subject={data_subject} | schema={source_schema} | object={source_object}")
print(f"load_type={load_type} | last_watermark={last_watermark}")
print(f"landing_path={landing_path}")

# COMMAND ----------

# MAGIC %md ## Build JDBC Connection

# COMMAND ----------

hana_host = dbutils.secrets.get(scope=SECRET_SCOPE, key="saphana-host")
hana_user = dbutils.secrets.get(scope=SECRET_SCOPE, key="saphana-user")
hana_pass = dbutils.secrets.get(scope=SECRET_SCOPE, key="saphana-password")
hana_port = "443"

jdbc_url = f"jdbc:sap://{hana_host}:{hana_port}?encrypt=true&validateCertificate=false"

# COMMAND ----------

# MAGIC %md ## Read from HANA via JDBC

# COMMAND ----------

full_table = f'"{source_schema}"."{source_object}"'

if load_type == "incremental" and watermark_col and last_watermark:
    query = f'SELECT * FROM {full_table} WHERE "{watermark_col}" > \'{last_watermark}\''
else:
    query = f"SELECT * FROM {full_table}"

print(f"Query: {query}")

try:
    raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("query", query)
        .option("user", hana_user)
        .option("password", hana_pass)
        .option("driver", "com.sap.db.jdbc.Driver")
        .load()
    )
    src_count = raw_df.count()
    print(f"Rows read from HANA: {src_count}")
except Exception as e:
    raise

# COMMAND ----------

if src_count == 0:
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# MAGIC %md ## Write Parquet to Landing

# COMMAND ----------

file_name  = f"{data_subject}_{load_job_nr}.parquet"
write_path = f"{landing_path}{file_name}"

try:
    (
        raw_df
        .coalesce(4)
        .write.format("parquet")
        .mode("overwrite")
        .save(write_path)
    )
    print(f"Written to landing: {write_path}")
except Exception as e:
    raise

# COMMAND ----------

# MAGIC %md ## Update Watermark + Audit

# COMMAND ----------

if load_type == "incremental" and new_watermark:
    spark.sql(f"""
        UPDATE {PIPELINE_CONFIG}
        SET    last_watermark = '{new_watermark}',
               updated_ts     = current_timestamp()
        WHERE  data_subject   = '{data_subject}' AND is_active = true
    """)
    print(f"Watermark updated to: {new_watermark}")

try:
    spark.sql(f"""
        INSERT INTO {JOB_LOG_TABLE} (batch_id, insert_time, layer, status,
            records_inserted, records_updated, error_records,
            source_system, data_subject, object_name)
        VALUES ('{batch_id}', current_timestamp(), 'landing', 'SUCCESS',
                {src_count}, 0, 0,
                '{source_system}', '{data_subject}', '{source_object}')
    """)
except Exception:
    pass  # audit failure must not fail the pipeline

dbutils.notebook.exit(
    f'{{"batch_id":"{batch_id}","src_count":{src_count},"landing_path":"{write_path}"}}'
)
