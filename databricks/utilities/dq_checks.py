"""
DQ Checks Utility — Strategy Pattern
Each check is a strategy: same interface, different behaviour.
Notebooks compose a list of checks and execute them in order.
Adding a new check = new class, zero notebook changes.
"""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


class DQCheck(ABC):
    """Base strategy. Every check returns (valid_df, invalid_df, result_dict)."""

    @abstractmethod
    def run(self, df: DataFrame) -> tuple:
        """
        Returns:
            valid_df   : rows that passed
            invalid_df : rows that failed (with _dq_fail_reason column added)
            result     : dict with check_type, records_checked, records_failed, column_name
        """
        pass


# ──────────────────────────────────────────────
# Concrete Strategy 1: PK Null Check
# ──────────────────────────────────────────────
class NullPKCheck(DQCheck):
    """Flags rows where any PK column is NULL or empty string."""

    def __init__(self, pk_cols: list):
        self.pk_cols = pk_cols

    def run(self, df: DataFrame) -> tuple:
        null_cond = F.lit(False)
        for col in self.pk_cols:
            if col in df.columns:
                null_cond = null_cond | F.col(col).isNull() | (F.trim(F.col(col)) == "")

        valid_df   = df.filter(~null_cond)
        invalid_df = df.filter(null_cond).withColumn("_dq_fail_reason", F.lit("NULL_PK"))

        return valid_df, invalid_df, {
            "check_type":      "NULL_PK",
            "column_name":     ",".join(self.pk_cols),
            "records_checked": df.count(),
            "records_failed":  invalid_df.count(),
        }


# ──────────────────────────────────────────────
# Concrete Strategy 2: Deduplication
# ──────────────────────────────────────────────
class DedupCheck(DQCheck):
    """Keeps only the latest row per PK + frequency partition."""

    def __init__(self, pk_cols: list, ts_col: str = "_ingestion_ts", freq_col: str = "_frequency"):
        self.pk_cols  = pk_cols
        self.ts_col   = ts_col
        self.freq_col = freq_col

    def run(self, df: DataFrame) -> tuple:
        partition_cols = self.pk_cols + ([self.freq_col] if self.freq_col in df.columns else [])
        w = Window.partitionBy(*partition_cols).orderBy(F.col(self.ts_col).desc())

        ranked    = df.withColumn("_rn", F.row_number().over(w))
        valid_df  = ranked.filter(F.col("_rn") == 1).drop("_rn")
        dups_df   = ranked.filter(F.col("_rn") > 1).drop("_rn") \
                          .withColumn("_dq_fail_reason", F.lit("DUPLICATE"))

        return valid_df, dups_df, {
            "check_type":      "DEDUP",
            "column_name":     ",".join(partition_cols),
            "records_checked": df.count(),
            "records_failed":  dups_df.count(),
        }


# ──────────────────────────────────────────────
# Concrete Strategy 3: Domain Value Check
# Reusable for category, currency, or any enum column.
# ──────────────────────────────────────────────
class DomainCheck(DQCheck):
    """
    Flags rows where column value is not in the allowed set.
    Rows are NOT dropped — they stay in valid_df with the violation
    recorded separately for the DQ log (Silver behaviour).
    """

    def __init__(self, column: str, valid_values: set, check_type: str = None):
        self.column      = column
        self.valid_values = valid_values
        self.check_type  = check_type or f"{column.upper()}_VALIDATION"

    def run(self, df: DataFrame) -> tuple:
        invalid_mask = (
            F.col(self.column).isNotNull() &
            ~F.col(self.column).isin(list(self.valid_values))
        )
        # Silver keeps all rows — invalid_df here is informational only (not quarantined)
        invalid_df = df.filter(invalid_mask)

        return df, invalid_df, {
            "check_type":      self.check_type,
            "column_name":     self.column,
            "records_checked": df.count(),
            "records_failed":  invalid_df.count(),
        }


# ──────────────────────────────────────────────
# Check Runner — executes a list of strategies
# ──────────────────────────────────────────────
def run_dq_checks(
    spark: SparkSession,
    df: DataFrame,
    checks: list,
    batch_id: str,
    layer: str,
    table_name: str,
    data_subject: str,
    quarantine_table: str = None,
) -> tuple:
    """
    Run a list of DQCheck strategies in order.
    Checks that produce invalid rows (NULL_PK, DEDUP) route them to quarantine.
    Checks that are informational only (DomainCheck) log but do not remove rows.

    Returns:
        clean_df        : DataFrame after all quarantine checks applied
        total_quarantined: int
        dq_results      : list of result dicts (one per check)
    """
    from utilities.audit_helper import log_dq_result

    current_df      = df
    total_quarantined = 0
    dq_results      = []

    for check in checks:
        valid_df, invalid_df, result = check.run(current_df)
        failed_count = result["records_failed"]

        log_dq_result(
            spark=spark, batch_id=batch_id, layer=layer,
            table_name=table_name, check_type=result["check_type"],
            column_name=result["column_name"],
            records_checked=result["records_checked"],
            records_failed=failed_count,
            data_subject=data_subject,
        )

        # Route to quarantine only for hard-fail checks (NULL_PK, DEDUP)
        # DomainCheck is informational — rows stay in the main flow
        if failed_count > 0 and quarantine_table and result["check_type"] in ("NULL_PK", "DUPLICATE"):
            (
                invalid_df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(quarantine_table)
            )
            total_quarantined += failed_count
            current_df = valid_df   # remove quarantined rows from main flow
        else:
            current_df = valid_df   # for DomainCheck valid_df == full df

        dq_results.append(result)
        print(f"  [DQ] {result['check_type']} | checked={result['records_checked']} failed={failed_count}")

    return current_df, total_quarantined, dq_results
