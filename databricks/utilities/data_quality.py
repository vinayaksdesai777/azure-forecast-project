# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Utility
# MAGIC Provides data quality validation functions.
# MAGIC Validates null checks, data types, duplicates, and empty strings.

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from typing import List, Tuple


def validate_not_null(df: DataFrame, columns: List[str]) -> Tuple[DataFrame, DataFrame]:
    """
    Validate that specified columns are not null or empty.
    
    Parameters:
        df: Input DataFrame
        columns: List of column names to check for nulls
    
    Returns:
        Tuple of (valid_df, invalid_df)
    """
    # Build condition: all specified columns must be non-null and non-empty
    valid_condition = F.lit(True)
    
    for col_name in columns:
        if col_name in df.columns:
            valid_condition = valid_condition & (
                F.col(col_name).isNotNull() & 
                (F.trim(F.col(col_name)).cast("string") != "")
            )
    
    valid_df = df.filter(valid_condition)
    invalid_df = df.filter(~valid_condition)
    
    return valid_df, invalid_df


def validate_data_types(df: DataFrame, expected_schema: dict) -> Tuple[DataFrame, DataFrame]:
    """
    Validate that columns can be cast to expected data types.
    
    Parameters:
        df: Input DataFrame
        expected_schema: Dict of {column_name: expected_type_string}
    
    Returns:
        Tuple of (valid_df, invalid_df)
    """
    # Add a validation flag column
    validated_df = df.withColumn("_type_valid", F.lit(True))
    
    for col_name, expected_type in expected_schema.items():
        if col_name in df.columns:
            # Try casting - if it produces null where original wasn't null, it's invalid
            validated_df = validated_df.withColumn(
                "_type_valid",
                F.col("_type_valid") & (
                    F.col(col_name).isNull() |
                    F.col(col_name).cast(expected_type).isNotNull()
                )
            )
    
    valid_df = validated_df.filter(F.col("_type_valid")).drop("_type_valid")
    invalid_df = validated_df.filter(~F.col("_type_valid")).drop("_type_valid")
    
    return valid_df, invalid_df


def validate_duplicates(df: DataFrame, key_columns: List[str]) -> Tuple[DataFrame, DataFrame]:
    """
    Identify and separate duplicate records based on key columns.
    Keeps the first occurrence and marks subsequent ones as duplicates.
    
    Parameters:
        df: Input DataFrame
        key_columns: List of columns that define uniqueness
    
    Returns:
        Tuple of (deduplicated_df, duplicates_df)
    """
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy(*key_columns).orderBy(F.monotonically_increasing_id())
    
    ranked_df = df.withColumn("_row_num", F.row_number().over(window_spec))
    
    deduplicated_df = ranked_df.filter(F.col("_row_num") == 1).drop("_row_num")
    duplicates_df = ranked_df.filter(F.col("_row_num") > 1).drop("_row_num")
    
    return deduplicated_df, duplicates_df


def nullify_empty_strings(df: DataFrame) -> DataFrame:
    """
    Replace empty strings and whitespace-only strings with null.
    Converts empty/whitespace-only strings to null for cleaner data.
    
    Parameters:
        df: Input DataFrame
    
    Returns:
        DataFrame with empty strings replaced by null
    """
    for col_name in df.columns:
        if df.schema[col_name].dataType == StringType():
            df = df.withColumn(
                col_name,
                F.when(
                    F.trim(F.col(col_name)) == "", None
                ).otherwise(F.col(col_name))
            )
    return df


def log_dq_results(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_properties: dict,
    batch_id: str,
    table_name: str,
    check_type: str,
    column_name: str,
    records_checked: int,
    records_failed: int,
    error_message: str = None
):
    """
    Log data quality check results to Azure SQL.
    
    Parameters:
        spark: SparkSession
        jdbc_url: JDBC URL for Azure SQL
        jdbc_properties: Connection properties
        batch_id: Current batch ID
        table_name: Table being validated
        check_type: Type of check (NULL_CHECK, DUPLICATE_CHECK, FORMAT_CHECK)
        column_name: Column(s) being checked
        records_checked: Total records evaluated
        records_failed: Records that failed the check
        error_message: Optional error description
    """
    check_status = "PASSED" if records_failed == 0 else "FAILED"
    
    dq_row = Row(
        batch_id=batch_id,
        table_name=table_name,
        check_type=check_type,
        column_name=column_name,
        records_checked=records_checked,
        records_failed=records_failed,
        check_status=check_status,
        error_message=error_message
    )
    
    dq_df = spark.createDataFrame([dq_row])
    
    (
        dq_df.write
        .jdbc(
            url=jdbc_url,
            table="audit.data_quality_log",
            mode="append",
            properties=jdbc_properties
        )
    )
    
    print(f"  [DQ] {check_type} on {table_name}.{column_name}: "
          f"{check_status} ({records_failed}/{records_checked} failed)")
