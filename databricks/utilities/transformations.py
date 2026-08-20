"""
Transformations Utility — Strategy Pattern
Each transformation is a strategy: same interface, different behaviour.
Notebooks compose a pipeline of transforms and apply them in sequence.
Adding a new transform = new class, zero notebook changes.
"""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession, Window
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
    storage_path: str = None,
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
        writer = (
            incoming_df
            .repartition(num_partitions)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("_ingestion_date")
        )
        if storage_path:
            writer = writer.option("path", storage_path)
        writer.saveAsTable(target_table)
        inserted = incoming_df.count()
        print(f"  [SCD2] First load — inserted: {inserted}")
        return inserted, 0

    # Deduplicate on merge keys — Delta MERGE requires at most one source row per target row
    incoming_df = incoming_df.withColumn("_mono_id", F.monotonically_increasing_id())
    _w = Window.partitionBy(*merge_keys).orderBy(F.col("_mono_id"))
    incoming_df = incoming_df.withColumn("_rn", F.row_number().over(_w)).filter(F.col("_rn") == 1).drop("_rn", "_mono_id")

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


def build_surrogate_key(df: DataFrame, natural_key: str, effective_from_col: str = "effective_from",
                        tracked_cols: list = None):
    """
    Deterministic surrogate key: sha2(natural_key || effective_from || tracked).

    Chosen over GENERATED ALWAYS AS IDENTITY because it is reproducible across
    full reloads and table recreation — the same business row always resolves to
    the same key, so the fact table stays valid if a dimension is rebuilt.

    effective_from alone does NOT separate versions: it has day granularity, so
    two versions of the same member created in one session hash identically and
    the dimension ends up with duplicate surrogate keys. A star join on
    product_sk then matches a fact row against every colliding version — that is
    how v_forecast_star reached 4,030,130 rows against 1,972,706 facts, while
    dim_location, which never churns, stayed exact.

    Mixing the tracked attributes into the hash makes the key unique per
    distinct version regardless of how many land on the same date.
    """
    parts = [F.col(natural_key).cast("string"), F.col(effective_from_col).cast("string")]
    for c in (tracked_cols or []):
        parts.append(F.coalesce(F.col(c).cast("string"), F.lit("")))
    return df.withColumn(
        natural_key.replace("_id", "") + "_sk",
        F.sha2(F.concat_ws("||", *parts), 256)
    )


def apply_dim_scd2_merge(
    spark: SparkSession,
    incoming_df: DataFrame,
    target_table: str,
    natural_key: str,
    tracked_cols: list,
    storage_path: str = None,
) -> tuple:
    """
    Two-pass SCD Type 2 merge for a dimension table.

    Step 1 expires active rows whose tracked attributes changed.
    Step 2 inserts new versions plus net-new members.

    The two passes are required: a single MERGE with whenMatchedUpdate +
    whenNotMatchedInsertAll expires the old row but never inserts the new
    version, because a changed member *matches* and so never reaches the
    not-matched branch. That silently removes the member from active rows.
    Step 2 negates the change condition so expired rows no longer match and
    the new version is inserted.

    Returns:
        inserted : int  (new versions + net-new members)
        expired  : int  (rows closed off this run)
    """
    if not spark.catalog.tableExists(target_table):
        writer = incoming_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if storage_path:
            writer = writer.option("path", storage_path)
        writer.saveAsTable(target_table)
        inserted = incoming_df.count()
        print(f"  [DIM SCD2] {target_table} first load — inserted: {inserted}")
        return inserted, 0

    target_dt   = DeltaTable.forName(spark, target_table)
    change_cond = " OR ".join([
        f"existing.{c} <> incoming.{c}"
        for c in tracked_cols if c in incoming_df.columns
    ])

    # Step 1 — expire changed active rows
    target_dt.alias("existing").merge(
        incoming_df.alias("incoming"),
        f"existing.{natural_key} = incoming.{natural_key} "
        f"AND existing.is_active = true AND ({change_cond})"
    ).whenMatchedUpdate(set={
        "effective_to": F.current_date(),
        "is_active":    F.lit(False),
    }).execute()

    expired = spark.table(target_table).filter(
        (F.col("is_active") == False) & (F.col("effective_to") == F.current_date())
    ).count()

    # Step 2 — insert new versions and net-new members.
    # Condition negated so rows expired above no longer match and do insert.
    target_dt.alias("existing").merge(
        incoming_df.alias("incoming"),
        f"existing.{natural_key} = incoming.{natural_key} "
        f"AND existing.is_active = true AND NOT ({change_cond})"
    ).whenNotMatchedInsertAll().execute()

    total = spark.table(target_table).count()
    print(f"  [DIM SCD2] {target_table} — expired: {expired} | total rows: {total}")
    return total, expired


def resolve_dim_sk(fact_df: DataFrame, dim_df: DataFrame, natural_key: str, sk_col: str) -> DataFrame:
    """
    Attach a dimension surrogate key to fact rows via the active dim version.

    Filtering to is_active = true is what makes SCD2 dimensions safe to join:
    without it a member with an expired and an active version matches twice and
    silently doubles every measure. Left join so fact rows survive a missing
    dimension member; unmatched rows get a sentinel key rather than NULL.
    """
    active_dim = (
        dim_df.filter(F.col("is_active") == True)
              .select(F.col(natural_key).alias("_dim_nk"), F.col(sk_col))
    )
    return (
        fact_df.join(active_dim, fact_df[natural_key] == active_dim["_dim_nk"], "left")
               .drop("_dim_nk")
               .withColumn(sk_col, F.coalesce(F.col(sk_col), F.lit("UNKNOWN")))
    )
