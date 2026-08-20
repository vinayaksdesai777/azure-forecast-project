# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Update Watermark
# MAGIC Called by ADF after each successful extract (replaces usp_update_extract_watermark stored proc).
# MAGIC Updates hpe_catalog.audit.pipeline_config with the new high-watermark value and
# MAGIC flips load_type to 'incremental' after the first full load completes.
# MAGIC
# MAGIC The watermark is the MAX of the watermark column in the data that actually
# MAGIC landed, not the wall clock. Using the clock (ADF's utcNow at pipeline start)
# MAGIC silently drops rows: a source row committed while the extract is running
# MAGIC carries a timestamp earlier than utcNow but is not in the result set, so the
# MAGIC next incremental run — filtering on > utcNow — skips it forever. Deriving the
# MAGIC watermark from the extracted rows means the next run resumes exactly where
# MAGIC this one stopped, and a row that arrives mid-extract is simply picked up next
# MAGIC time. The clock value is kept only as a fallback for a source with no
# MAGIC watermark column.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

spark.sql("USE CATALOG hpe_catalog")

# Same local-helper pattern as 01_ingest_hana_to_landing, so this notebook
# stands alone without a %run of 00_config.
SECRET_SCOPE      = "hpe-forecast"
STORAGE_ACCOUNT   = dbutils.secrets.get(scope=SECRET_SCOPE, key="adls-account-name")
CONTAINER_LANDING = "landing"


def get_adls_path(container, path=""):
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"

# COMMAND ----------

dbutils.widgets.text("data_subject",   "", "Data Subject")
dbutils.widgets.text("new_watermark",  "", "Fallback watermark (yyyy-MM-dd HH:mm:ss), used only if the landed data has none")
dbutils.widgets.text("run_id",         "", "ADF Pipeline Run ID")

data_subject      = dbutils.widgets.get("data_subject").strip()
fallback_watermark = dbutils.widgets.get("new_watermark").strip()

if not data_subject:
    raise ValueError("data_subject widget is required")
if not fallback_watermark:
    raise ValueError("new_watermark widget is required")

# COMMAND ----------

cfg = spark.sql(f"""
    SELECT watermark_column, landing_path, last_watermark
    FROM   hpe_catalog.audit.pipeline_config
    WHERE  data_subject = '{data_subject}' AND is_active = true
""").first()

if not cfg:
    raise ValueError(f"No active pipeline_config row for data_subject='{data_subject}'")

watermark_col = cfg["watermark_column"]

if not watermark_col:
    # Source has no watermark column, so there is nothing to advance.
    print(f"data_subject={data_subject} has no watermark_column — nothing to update.")
    dbutils.notebook.exit(f"SKIPPED|{data_subject}|no watermark column")

# Read back what this extract landed and take the real high-water mark.
landing_glob = get_adls_path(CONTAINER_LANDING, cfg["landing_path"]).rstrip("/") + "/*.parquet"

try:
    landed = spark.read.format("parquet").load(landing_glob)
    landed = landed.toDF(*[c.lower() for c in landed.columns])
    wm_col = watermark_col.lower()

    if wm_col in landed.columns:
        observed = landed.agg(F.max(F.col(wm_col).cast("string"))).first()[0]
    else:
        # The extract does not project its watermark column into landing — the
        # Salesforce SOQL selects the business fields but not LastModifiedDate,
        # for instance. Fall back to the clock and say so, rather than failing a
        # load that otherwise succeeded.
        observed = None
        print(
            f"WARNING: watermark_column '{watermark_col}' is not in the landed data for "
            f"{data_subject} (columns: {sorted(landed.columns)}). Falling back to the "
            f"clock value, which can skip rows committed during the extract. Project the "
            f"column into landing to make the watermark exact."
        )
except AnalysisException:
    # Landing already cleared, or nothing landed this cycle. Leave the stored
    # watermark alone rather than advancing past data that was never read.
    observed = None

if observed:
    new_watermark = observed
    source = "landed data"
else:
    new_watermark = fallback_watermark
    source = "fallback clock value"

print(f"Watermark for {data_subject} derived from {source}: {new_watermark}")

# Never move the watermark backwards: a re-run that lands an older slice must
# not cause the next incremental to re-read rows already processed.
prior = cfg["last_watermark"]
if prior and str(new_watermark) < str(prior):
    print(f"Observed watermark {new_watermark} is older than stored {prior} — keeping {prior}.")
    new_watermark = prior

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
