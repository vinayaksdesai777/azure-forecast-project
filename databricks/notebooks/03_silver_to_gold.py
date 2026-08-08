# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver to Gold (Star Schema)
# MAGIC Builds conformed dimensions (product, location, time) via two-pass SCD2 merge,
# MAGIC resolves surrogate keys onto the fact, then rebuilds the fact slice for this
# MAGIC frequency with replaceWhere.
# MAGIC
# MAGIC The fact is a PERIODIC SNAPSHOT: a restated forecast replaces the prior value
# MAGIC for its grain. replaceWhere scopes the rebuild to this subject's _frequency so
# MAGIC the other three subjects sharing the table are untouched.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# sys.path for utilities/ is set by 00_config (%run above), derived from the
# notebook's own location so it works from any clone path.
from pyspark.sql import functions as F
from pyspark.sql import Window
from utilities.audit_helper import write_audit_entry, mark_audit_failed
from utilities.transformations import (build_surrogate_key, apply_dim_scd2_merge,
                                       resolve_dim_sk)

# COMMAND ----------

dbutils.widgets.text("data_subject", "", "Data Subject")
dbutils.widgets.text("batch_id",     "", "Batch ID from Silver")
dbutils.widgets.text("run_id",       "", "ADF Pipeline Run ID")

# .strip() — widget values keep whatever was pasted, including trailing spaces.
data_subject      = dbutils.widgets.get("data_subject").strip()
upstream_batch_id = dbutils.widgets.get("batch_id").strip()
run_id            = dbutils.widgets.get("run_id").strip()

# COMMAND ----------

metadata       = get_pipeline_metadata(data_subject)
source_system  = metadata["source_system"]
silver_table   = metadata["silver_table"]
frequency      = metadata["frequency"]
partition_col  = metadata.get("partition_column", "forecast_date")
apply_concat   = str(metadata.get("apply_concat", "true")).lower() == "true"
num_partitions = int(metadata.get("num_partitions") or DEFAULT_NUM_PARTITIONS)

# Star schema targets. The fact name is fixed rather than read from
# pipeline_config.gold_table, which still points at the pre-star table.
fact_table         = f"{GOLD_SCHEMA}.fact_forecast"
dim_product_table  = f"{GOLD_SCHEMA}.dim_product"
dim_location_table = f"{GOLD_SCHEMA}.dim_location"
dim_time_table     = f"{GOLD_SCHEMA}.dim_time"

batch_id    = get_batch_id(f"gold_{data_subject}")
load_job_nr = get_timestamp()

print(f"Source    : {silver_table}  (frequency = {frequency})")
print(f"Fact      : {fact_table}")
print(f"batch_id  : {batch_id}")

# COMMAND ----------

# MAGIC %md ## Read Silver (current version of each record)

# COMMAND ----------

# is_active = true gives the current SCD2 version of every Silver record, which
# is what a periodic-snapshot fact should reflect.
silver_df = (
    spark.table(silver_table)
    .filter(F.col("is_active") == True)
    .filter(F.col("_frequency") == frequency)
)
src_count = silver_df.count()
print(f"Silver active records for {frequency}: {src_count}")

if src_count == 0:
    write_audit_entry(spark, batch_id=batch_id, layer="gold", status="SUCCESS",
                      records_inserted=0, source_system=source_system,
                      data_subject=data_subject, object_name=fact_table, run_id=run_id)
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# MAGIC %md ## Derive Period

# COMMAND ----------

if apply_concat and partition_col in silver_df.columns:
    gold_df = silver_df.withColumn("period", F.date_format(F.col(partition_col), "yyyy-MM-01"))
else:
    gold_df = silver_df.withColumn("period", F.col(partition_col).cast("string"))

# COMMAND ----------

# MAGIC %md ## Dimension: Product (two-pass SCD2)

# COMMAND ----------

# One row per product_id. A product_id can appear with several category values
# across source rows, so dedup first: Delta MERGE rejects multiple source rows
# matching the same target row.
_prod_w = Window.partitionBy("product_id").orderBy(F.col("_mono_id"))
dim_product_src = (
    gold_df
    .withColumn("_mono_id", F.monotonically_increasing_id())
    .withColumn("_rn", F.row_number().over(_prod_w))
    .filter(F.col("_rn") == 1)
    .select("product_id", "category", "sub_category")
    .withColumn("product_name",   F.lit(None).cast("string"))
    .withColumn("is_active",      F.lit(True))
    .withColumn("effective_from", F.current_date())
    .withColumn("effective_to",   F.lit(None).cast("date"))
    .withColumn("_load_ts",       F.current_timestamp())
)
dim_product_src = build_surrogate_key(dim_product_src, "product_id")

apply_dim_scd2_merge(
    spark=spark, incoming_df=dim_product_src, target_table=dim_product_table,
    natural_key="product_id", tracked_cols=["category", "sub_category"],
    storage_path=get_adls_path("gold", "dim_product"),
)

# COMMAND ----------

# MAGIC %md ## Dimension: Location (two-pass SCD2)

# COMMAND ----------

_loc_w = Window.partitionBy("location_id").orderBy(F.col("_mono_id"))
dim_location_src = (
    gold_df
    .withColumn("_mono_id", F.monotonically_increasing_id())
    .withColumn("_rn", F.row_number().over(_loc_w))
    .filter(F.col("_rn") == 1)
    .select("location_id", "region", "country")
    .withColumn("city",           F.lit(None).cast("string"))
    .withColumn("is_active",      F.lit(True))
    .withColumn("effective_from", F.current_date())
    .withColumn("effective_to",   F.lit(None).cast("date"))
    .withColumn("_load_ts",       F.current_timestamp())
)
dim_location_src = build_surrogate_key(dim_location_src, "location_id")

apply_dim_scd2_merge(
    spark=spark, incoming_df=dim_location_src, target_table=dim_location_table,
    natural_key="location_id", tracked_cols=["region", "country"],
    storage_path=get_adls_path("gold", "dim_location"),
)

# COMMAND ----------

# MAGIC %md ## Dimension: Time

# COMMAND ----------

# Not versioned — a calendar date's attributes never change, so insert-only.
dim_time_src = (
    gold_df
    .select(F.col("forecast_date").alias("date_key"))
    .distinct()
    .withColumn("year",           F.year("date_key"))
    .withColumn("quarter",        F.quarter("date_key"))
    .withColumn("month",          F.month("date_key"))
    .withColumn("month_name",     F.date_format("date_key", "MMMM"))
    .withColumn("week_of_year",   F.weekofyear("date_key"))
    .withColumn("day_of_week",    F.dayofweek("date_key"))
    .withColumn("day_name",       F.date_format("date_key", "EEEE"))
    .withColumn("is_weekend",     F.dayofweek("date_key").isin([1, 7]))
    .withColumn("fiscal_year",    F.year("date_key"))
    .withColumn("fiscal_quarter", F.quarter("date_key"))
    .withColumn("period",         F.date_format("date_key", "yyyy-MM-01"))
)

if not spark.catalog.tableExists(dim_time_table):
    (dim_time_src.write.format("delta").mode("overwrite")
     .option("path", get_adls_path("gold", "dim_time")).saveAsTable(dim_time_table))
else:
    from delta.tables import DeltaTable
    DeltaTable.forName(spark, dim_time_table).alias("existing").merge(
        dim_time_src.alias("incoming"), "existing.date_key = incoming.date_key"
    ).whenNotMatchedInsertAll().execute()

print(f"dim_time rows: {spark.table(dim_time_table).count()}")

# COMMAND ----------

# MAGIC %md ## Resolve Surrogate Keys onto the Fact

# COMMAND ----------

fact_df = gold_df.withColumn("date_key", F.col("forecast_date"))

# Join to the ACTIVE dimension version only. Without that filter a member with
# an expired and a current version matches twice and doubles every measure.
fact_df = resolve_dim_sk(fact_df, spark.table(dim_product_table),  "product_id",  "product_sk")
fact_df = resolve_dim_sk(fact_df, spark.table(dim_location_table), "location_id", "location_sk")

unresolved = fact_df.filter(
    (F.col("product_sk") == "UNKNOWN") | (F.col("location_sk") == "UNKNOWN")
).count()
if unresolved:
    print(f"WARNING: {unresolved} fact rows have an unresolved dimension key")

# COMMAND ----------

# MAGIC %md ## Build Fact Rows

# COMMAND ----------

FACT_COLS = [
    "product_sk", "location_sk", "date_key",
    "channel", "customer_id",
    "product_id", "location_id", "forecast_date",
    "forecast_qty", "revenue_amount", "currency", "uom",
    "period", "_frequency", "_gold_load_ts", "_batch_id",
]

fact_df = (
    fact_df
    .withColumn("_gold_load_ts", F.current_timestamp())
    .withColumn("_batch_id",     F.lit(batch_id))
)

# Collapse to one row per grain. Deterministic tie-break on _silver_load_ts so a
# rerun of the same Silver data produces the same fact.
grain = ["product_id", "location_id", "forecast_date", "channel", "customer_id", "_frequency"]
_order = F.col("_silver_load_ts").desc() if "_silver_load_ts" in fact_df.columns \
         else F.col("_mono_id").asc()
fact_df = (
    fact_df
    .withColumn("_mono_id", F.monotonically_increasing_id())
    .withColumn("_rn", F.row_number().over(Window.partitionBy(*grain).orderBy(_order)))
    .filter(F.col("_rn") == 1)
    .select(*FACT_COLS)
)

records_inserted = fact_df.count()
print(f"Fact rows for {frequency}: {records_inserted}")

# COMMAND ----------

# MAGIC %md ## Write Fact — replaceWhere scoped to this frequency

# COMMAND ----------

try:
    if not spark.catalog.tableExists(fact_table):
        # First write establishes the partitioning every later subject depends on.
        (
            fact_df.repartition(num_partitions)
            .write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .option("path", get_adls_path("gold", "fact_forecast"))
            .partitionBy("period", "_frequency")
            .saveAsTable(fact_table)
        )
        print(f"First load — inserted: {records_inserted}")
    else:
        # Replace only this subject's slice. A plain overwrite would drop the
        # other three frequencies sharing the table; an insert-only MERGE would
        # silently drop restatements.
        (
            fact_df.repartition(num_partitions)
            .write.format("delta").mode("overwrite")
            .option("replaceWhere", f"_frequency = '{frequency}'")
            .saveAsTable(fact_table)
        )
        print(f"Rebuilt {frequency} slice — rows: {records_inserted}")

    print(f"Fact total rows (all frequencies): {spark.table(fact_table).count()}")

except Exception as e:
    mark_audit_failed(spark, batch_id=batch_id, layer="gold", object_name=fact_table,
                      error_message=str(e), source_system=source_system,
                      data_subject=data_subject, run_id=run_id)
    raise

# COMMAND ----------

# MAGIC %md ## Write Audit Entry

# COMMAND ----------

write_audit_entry(
    spark=spark, batch_id=batch_id, layer="gold", status="SUCCESS",
    records_inserted=records_inserted, records_updated=0,
    source_system=source_system, data_subject=data_subject,
    object_name=fact_table, run_id=run_id
)

dbutils.notebook.exit(f'{{"batch_id":"{batch_id}","src_count":{src_count},"gold_count":{records_inserted}}}')
