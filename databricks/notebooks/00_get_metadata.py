# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Get Metadata
# MAGIC Called by ADF as a Lookup replacement.
# MAGIC Queries hpe_catalog.audit.pipeline_config (Unity Catalog Delta) and returns
# MAGIC the active extract rows as a JSON string via dbutils.notebook.exit().
# MAGIC ADF reads the result with @json(activity('Get Metadata').output.runOutput).
# MAGIC
# MAGIC No JDBC. No Azure SQL. Unity Catalog only.

# COMMAND ----------

import json

spark.sql("USE CATALOG hpe_catalog")

# COMMAND ----------

dbutils.widgets.text("data_subject", "", "Filter by data_subject (empty = all active)")
data_subject_filter = dbutils.widgets.get("data_subject").strip()

# COMMAND ----------

if data_subject_filter:
    rows = spark.sql(f"""
        SELECT source_system, connector, source_schema, source_object,
               watermark_column, landing_path, load_type, last_watermark,
               data_subject
        FROM   hpe_catalog.audit.pipeline_config
        WHERE  is_active = true
          AND  data_subject = '{data_subject_filter}'
    """).collect()
else:
    rows = spark.sql("""
        SELECT source_system, connector, source_schema, source_object,
               watermark_column, landing_path, load_type, last_watermark,
               data_subject
        FROM   hpe_catalog.audit.pipeline_config
        WHERE  is_active = true
    """).collect()

if not rows:
    raise ValueError(f"No active pipeline_config rows found for filter='{data_subject_filter}'")

result = [row.asDict() for row in rows]
print(f"Returning {len(result)} config rows to ADF")

# ADF reads this via @json(activity('Get Metadata').output.runOutput)
dbutils.notebook.exit(json.dumps(result))
