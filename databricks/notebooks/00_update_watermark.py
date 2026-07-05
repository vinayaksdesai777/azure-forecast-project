# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Update Watermark
# MAGIC Called by ADF after each successful extract (replaces usp_update_extract_watermark stored proc).
# MAGIC Updates hpe_catalog.audit.pipeline_config with the new high-watermark value and
# MAGIC flips load_type to 'incremental' after the first full load completes.

# COMMAND ----------

spark.sql("USE CATALOG hpe_catalog")

# COMMAND ----------

dbutils.widgets.text("data_subject",   "", "Data Subject")
dbutils.widgets.text("new_watermark",  "", "New Watermark Value (yyyy-MM-dd HH:mm:ss)")
dbutils.widgets.text("run_id",         "", "ADF Pipeline Run ID")

data_subject  = dbutils.widgets.get("data_subject").strip()
new_watermark = dbutils.widgets.get("new_watermark").strip()

if not data_subject:
    raise ValueError("data_subject widget is required")
if not new_watermark:
    raise ValueError("new_watermark widget is required")

# COMMAND ----------

spark.sql(f"""
    UPDATE hpe_catalog.audit.pipeline_config
    SET    last_watermark = '{new_watermark}',
           load_type      = 'incremental',
           updated_ts     = current_timestamp()
    WHERE  data_subject   = '{data_subject}'
      AND  watermark_column IS NOT NULL
""")

updated = spark.sql(f"""
    SELECT last_watermark, load_type
    FROM   hpe_catalog.audit.pipeline_config
    WHERE  data_subject = '{data_subject}'
""").first()

print(f"Watermark updated — data_subject={data_subject} | new_watermark={updated['last_watermark']} | load_type={updated['load_type']}")
dbutils.notebook.exit(f"OK|{data_subject}|{new_watermark}")
