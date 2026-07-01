# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver to Gold
# MAGIC Populates dim_product, dim_location, dim_time via SCD2 MERGE.
# MAGIC Loads periodic fact table via MERGE (never overwrites history).
# MAGIC Archives processed Silver partitions.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from utilities.audit_helper import write_audit_entry, mark_audit_failed

# COMMAND ----------

dbutils.widgets.text("data_subject", "", "Data Subject")
dbutils.widgets.text("batch_id",     "", "Batch ID from Silver")
dbutils.widgets.text("run_id",       "", "ADF Pipeline Run ID")

data_subject      = dbutils.widgets.get("data_subject")
upstream_batch_id = dbutils.widgets.get("batch_id")
run_id            = dbutils.widgets.get("run_id")

# COMMAND ----------

metadata       = get_pipeline_metadata(data_subject)
source_system  = metadata["source_system"]
silver_table   = metadata["silver_table"]
gold_table     = metadata["gold_table"]
partition_col  = metadata.get("partition_column", "forecast_date")
apply_concat   = str(metadata.get("apply_concat", "true")).lower() == "true"
num_partitions = int(metadata.get("num_partitions") or DEFAULT_NUM_PARTITIONS)

batch_id    = get_batch_id(f"gold_{data_subject}")
load_job_nr = get_timestamp()

print(f"Source : {silver_table}")
print(f"Target : {gold_table}")
print(f"batch_id: {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver (active records only)

# COMMAND ----------

silver_df = spark.table(silver_table).filter(F.col("is_active") == True)
src_count = silver_df.count()
print(f"Silver active records: {src_count}")

if src_count == 0:
    write_audit_entry(spark, batch_id=batch_id, layer="gold", status="SUCCESS",
                      records_inserted=0, source_system=source_system,
                      data_subject=data_subject, object_name=gold_table, run_id=run_id)
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive Period Column

# COMMAND ----------

if apply_concat and partition_col in silver_df.columns:
    gold_df = silver_df.withColumn("period", F.date_format(F.col(partition_col), "yyyy-MM-01"))
else:
    gold_df = silver_df.withColumn("period", F.col(partition_col).cast("string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Populate dim_product (SCD2 MERGE)

# COMMAND ----------

dim_product_src = (
    gold_df
    .select("product_id", "category", "sub_category")
    .distinct()
    .withColumn("is_active",     F.lit(True))
    .withColumn("effective_from", F.current_date())
    .withColumn("effective_to",   F.lit(None).cast("date"))
    .withColumn("_load_ts",       F.current_timestamp())
)

dim_product_table = f"{GOLD_SCHEMA}.dim_product"

if not spark.catalog.tableExists(dim_product_table):
    dim_product_src.write.format("delta").mode("overwrite").saveAsTable(dim_product_table)
else:
    DeltaTable.forName(spark, dim_product_table).alias("existing").merge(
        dim_product_src.alias("incoming"),
        "existing.product_id = incoming.product_id AND existing.is_active = true"
    ).whenMatchedUpdate(
        condition="existing.category <> incoming.category OR existing.sub_category <> incoming.sub_category",
        set={"effective_to": F.current_date(), "is_active": F.lit(False)}
    ).whenNotMatchedInsertAll().execute()

print(f"dim_product updated: {spark.table(dim_product_table).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Populate dim_location (SCD2 MERGE)

# COMMAND ----------

dim_location_src = (
    gold_df
    .select("location_id", "region", "country")
    .distinct()
    .withColumn("is_active",      F.lit(True))
    .withColumn("effective_from", F.current_date())
    .withColumn("effective_to",   F.lit(None).cast("date"))
    .withColumn("_load_ts",       F.current_timestamp())
)

dim_location_table = f"{GOLD_SCHEMA}.dim_location"

if not spark.catalog.tableExists(dim_location_table):
    dim_location_src.write.format("delta").mode("overwrite").saveAsTable(dim_location_table)
else:
    DeltaTable.forName(spark, dim_location_table).alias("existing").merge(
        dim_location_src.alias("incoming"),
        "existing.location_id = incoming.location_id AND existing.is_active = true"
    ).whenMatchedUpdate(
        condition="existing.region <> incoming.region OR existing.country <> incoming.country",
        set={"effective_to": F.current_date(), "is_active": F.lit(False)}
    ).whenNotMatchedInsertAll().execute()

print(f"dim_location updated: {spark.table(dim_location_table).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Populate dim_time

# COMMAND ----------

dim_time_table = f"{GOLD_SCHEMA}.dim_time"

dim_time_src = (
    gold_df
    .select(F.col("forecast_date").alias("date_key"))
    .distinct()
    .withColumn("year",          F.year("date_key"))
    .withColumn("quarter",       F.quarter("date_key"))
    .withColumn("month",         F.month("date_key"))
    .withColumn("month_name",    F.date_format("date_key", "MMMM"))
    .withColumn("week_of_year",  F.weekofyear("date_key"))
    .withColumn("day_of_week",   F.dayofweek("date_key"))
    .withColumn("day_name",      F.date_format("date_key", "EEEE"))
    .withColumn("is_weekend",    F.dayofweek("date_key").isin([1, 7]))
    .withColumn("fiscal_year",   F.year("date_key"))
    .withColumn("fiscal_quarter", F.quarter("date_key"))
    .withColumn("period",        F.date_format("date_key", "yyyy-MM-01"))
)

if not spark.catalog.tableExists(dim_time_table):
    dim_time_src.write.format("delta").mode("overwrite").saveAsTable(dim_time_table)
else:
    DeltaTable.forName(spark, dim_time_table).alias("existing").merge(
        dim_time_src.alias("incoming"),
        "existing.date_key = incoming.date_key"
    ).whenNotMatchedInsertAll().execute()

print(f"dim_time updated: {spark.table(dim_time_table).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE into Periodic Fact Table (never overwrite history)

# COMMAND ----------

fact_df = (
    gold_df
    .withColumn("_gold_load_ts", F.current_timestamp())
    .withColumn("_batch_id",     F.lit(batch_id))
)

fact_merge_keys = ["product_id", "location_id", "forecast_date", "channel", "_frequency"]

try:
    if not spark.catalog.tableExists(gold_table):
        (
            fact_df
            .repartition(num_partitions)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("period", "_frequency")
            .saveAsTable(gold_table)
        )
        records_inserted = fact_df.count()
        print(f"First load — inserted: {records_inserted}")
    else:
        merge_condition = " AND ".join([f"existing.{k} = incoming.{k}" for k in fact_merge_keys])
        DeltaTable.forName(spark, gold_table).alias("existing").merge(
            fact_df.alias("incoming"),
            merge_condition
        ).whenNotMatchedInsertAll().execute()   # never update — historical periods are immutable

        records_inserted = fact_df.count()
        print(f"Fact MERGE — new records inserted: {records_inserted}")

    tgt_count = spark.table(gold_table).count()
    print(f"Gold fact total rows: {tgt_count}")

except Exception as e:
    mark_audit_failed(spark, batch_id=batch_id, layer="gold", object_name=gold_table,
                      error_message=str(e), source_system=source_system,
                      data_subject=data_subject, run_id=run_id)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Archive Silver Partition

# COMMAND ----------

archive_path = get_adls_path(CONTAINER_ARCHIVE, f"silver/{data_subject}/{load_job_nr}/")
silver_path  = get_adls_path("silver", data_subject)
dbutils.fs.cp(silver_path, archive_path, recurse=True)
print(f"Silver archived to: {archive_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Audit Entry

# COMMAND ----------

write_audit_entry(
    spark=spark, batch_id=batch_id, layer="gold", status="SUCCESS",
    records_inserted=records_inserted, records_updated=0,
    source_system=source_system, data_subject=data_subject,
    object_name=gold_table, run_id=run_id
)

# COMMAND ----------

dbutils.notebook.exit(f'{{"batch_id":"{batch_id}","src_count":{src_count},"gold_count":{records_inserted}}}')
