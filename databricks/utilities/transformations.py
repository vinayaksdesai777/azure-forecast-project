# Databricks notebook source
# MAGIC %md
# MAGIC # Transformations Utility — Strategy Pattern
# MAGIC Each transformation is a strategy: same interface, different behaviour.
# MAGIC Notebooks compose a pipeline of transforms and apply them in sequence.
# MAGIC Adding a new transform = new class, zero notebook changes.

# COMMAND ----------

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable


class Transformation(ABC):
    """Base strategy. Every transform takes a DataFrame and returns a DataFrame."""

    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        pass


# ──────────────────────────────────────────────
# Concrete Strategy 1: Standardize
# Bronze layer — TRIM strings, UPPER(category), add audit columns
# ──────────────────────────────────────────────
class StandardizeTransform(Transformation):
    """
    Trims all non-audit string columns.
    Uppercases the category column if present.
    """

    def __init__(self, upper_cols: list = None):
        self.upper_cols = upper_cols or ["category"]

    def apply(self, df: DataFrame) -> DataFrame:
        string_cols = [
            f.name for f in df.schema.fields
            if str(f.dataType) == "StringType()" and not f.name.startswith("_")
        ]
        result = df
        for col in string_cols:
            result = result.withColumn(col, F.trim(F.col(col)))
        for col in self.upper_cols:
            if col in string_cols:
                result = result.withColumn(col, F.upper(F.col(col)))
        return result


# ──────────────────────────────────────────────
# Concrete Strategy 2: Add Audit Columns
# Bronze layer — injects pipeline lineage columns
# ──────────────────────────────────────────────
class AddAuditColumnsTransform(Transformation):
    """Adds _file_name, _ingestion_ts, _update_ts, _frequency, _load_job_nr, _batch_id."""

    def __init__(self, frequency: str, load_job_nr: str, batch_id: str):
        self.frequency   = frequency
        self.load_job_nr = load_job_nr
        self.batch_id    = batch_id

    def apply(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("_file_name",   F.element_at(F.split(F.input_file_name(), "/"), -1))
            .withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_update_ts",    F.current_timestamp())
            .withColumn("_frequency",    F.lit(self.frequency))
            .withColumn("_load_job_nr",  F.lit(self.load_job_nr))
            .withColumn("_batch_id",     F.lit(self.batch_id))
        )


# ──────────────────────────────────────────────
# Concrete Strategy 3: Type Cast
# Silver layer — coerce strings to business types
# ──────────────────────────────────────────────
class TypeCastTransform(Transformation):
    """
    Coerces Bronze string columns to typed Silver columns.
    Empty strings become NULL first.
    """

    def apply(self, df: DataFrame) -> DataFrame:
        # Empty string → NULL for all string columns
        result = df
        for field in df.schema.fields:
            if str(field.dataType) == "StringType()":
                result = result.withColumn(
                    field.name,
                    F.when(F.trim(F.col(field.name)) == "", None).otherwise(F.col(field.name))
                )
        # Cast to business types
        return (
            result
            .withColumn("forecast_date",  F.to_date("forecast_date"))
            .withColumn("forecast_qty",   F.col("forecast_qty").cast("decimal(12,2)"))
            .withColumn("revenue_amount", F.col("revenue_amount").cast("decimal(16,2)"))
        )


# ──────────────────────────────────────────────
# Concrete Strategy 4: Add Silver Metadata
# Silver layer — adds load timestamps + SCD2 columns
# ──────────────────────────────────────────────
class AddSilverMetaTransform(Transformation):
    """Adds _ingestion_date, _silver_load_ts, _batch_id, effective_from, effective_to, is_active."""

    def __init__(self, batch_id: str):
        self.batch_id = batch_id

    def apply(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("_ingestion_date", F.current_date())
            .withColumn("_silver_load_ts", F.current_timestamp())
            .withColumn("_batch_id",       F.lit(self.batch_id))
            .withColumn("effective_from",  F.current_date())
            .withColumn("effective_to",    F.lit(None).cast("date"))
            .withColumn("is_active",       F.lit(True))
        )


# ──────────────────────────────────────────────
# Concrete Strategy 5: Period Derivation
# Silver/Gold — derives yyyy-MM-01 period column
# ──────────────────────────────────────────────
class DerivePeriodTransform(Transformation):
    """Derives a 'period' column (yyyy-MM-01) from a date column."""

    def __init__(self, date_col: str = "forecast_date"):
        self.date_col = date_col

    def apply(self, df: DataFrame) -> DataFrame:
        return df.withColumn("period", F.date_format(F.col(self.date_col), "yyyy-MM-01"))


# ──────────────────────────────────────────────
# Transform Pipeline Runner
# ──────────────────────────────────────────────
def apply_transforms(df: DataFrame, transforms: list) -> DataFrame:
    """
    Apply a list of Transformation strategies in sequence.
    Each strategy receives the output of the previous one.
    """
    result = df
    for t in transforms:
        result = t.apply(result)
        print(f"  [TRANSFORM] {t.__class__.__name__} applied")
    return result


# ──────────────────────────────────────────────
# SCD Type 2 Merge — not a simple transform,
# but encapsulated here as a reusable operation
# ──────────────────────────────────────────────
def apply_scd2_merge(
    spark: SparkSession,
    incoming_df: DataFrame,
    target_table: str,
    merge_keys: list,
    tracked_cols: list,
    num_partitions: int = 8,
) -> tuple:
    """
    Performs a two-step SCD Type 2 MERGE into target_table.
    Step 1: expire changed active rows (set effective_to = today, is_active = False)
    Step 2: insert new rows for changed + net-new records

    Returns:
        records_inserted : int
        records_updated  : int (rows expired)
    """
    if not spark.catalog.tableExists(target_table):
        (
            incoming_df
            .repartition(num_partitions)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("_ingestion_date")
            .saveAsTable(target_table)
        )
        inserted = incoming_df.count()
        print(f"  [SCD2] First load — inserted: {inserted}")
        return inserted, 0

    target_dt     = DeltaTable.forName(spark, target_table)
    merge_cond    = " AND ".join([f"existing.{k} = incoming.{k}" for k in merge_keys])
    change_cond   = " OR ".join([
        f"existing.{c} <> incoming.{c}"
        for c in tracked_cols if c in incoming_df.columns
    ])

    # Step 1 — expire changed rows
    target_dt.alias("existing").merge(
        incoming_df.alias("incoming"),
        f"{merge_cond} AND existing.is_active = true AND ({change_cond})"
    ).whenMatchedUpdate(set={
        "effective_to": F.current_date(),
        "is_active":    F.lit(False)
    }).execute()

    updated = spark.table(target_table).filter(
        (F.col("is_active") == False) & (F.col("effective_to") == F.current_date())
    ).count()

    # Step 2 — insert new/changed rows
    target_dt.alias("existing").merge(
        incoming_df.alias("incoming"),
        f"{merge_cond} AND existing.is_active = true AND NOT ({change_cond})"
    ).whenNotMatchedInsertAll().execute()

    inserted = incoming_df.count() - updated
    print(f"  [SCD2] inserted={inserted} | expired={updated}")
    return inserted, updated
