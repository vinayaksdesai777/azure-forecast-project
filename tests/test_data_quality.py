"""
Unit tests for data quality validation functions.
Run with: pytest tests/test_data_quality.py
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("DataQualityTests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_data(spark):
    """Create sample test data."""
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("location_id", StringType(), True),
        StructField("forecast_date", StringType(), True),
        StructField("forecast_qty", StringType(), True),
        StructField("revenue_amount", StringType(), True),
    ])
    
    data = [
        ("P001", "L001", "2024-01-15", "100.5", "5000.00"),
        ("P002", "L002", "2024-01-16", "200.0", "10000.00"),
        (None, "L003", "2024-01-17", "50.0", "2500.00"),       # null product_id
        ("P004", "", "2024-01-18", "75.0", "3750.00"),          # empty location_id
        ("P005", "L005", None, "125.0", "6250.00"),             # null forecast_date
        ("P006", "L006", "2024-01-20", "", ""),                 # empty metrics (valid PKs)
        ("", "L007", "2024-01-21", "300.0", "15000.00"),        # empty product_id
    ]
    
    return spark.createDataFrame(data, schema)


class TestValidateNotNull:
    """Tests for null/empty validation on primary key columns."""
    
    def test_valid_records_pass(self, spark, sample_data):
        """Records with all PK values present should pass."""
        pk_columns = ["product_id", "location_id", "forecast_date"]
        
        valid_condition = F.lit(True)
        for col_name in pk_columns:
            valid_condition = valid_condition & (
                F.col(col_name).isNotNull() & 
                (F.trim(F.col(col_name)) != "")
            )
        
        valid_df = sample_data.filter(valid_condition)
        assert valid_df.count() == 3  # P001, P002, P006 have all PKs filled
    
    def test_null_records_filtered(self, spark, sample_data):
        """Records with null PK values should be filtered out."""
        pk_columns = ["product_id", "location_id", "forecast_date"]
        
        valid_condition = F.lit(True)
        for col_name in pk_columns:
            valid_condition = valid_condition & (
                F.col(col_name).isNotNull() & 
                (F.trim(F.col(col_name)) != "")
            )
        
        invalid_df = sample_data.filter(~valid_condition)
        assert invalid_df.count() == 4  # Rows with null/empty PKs
    
    def test_empty_string_treated_as_null(self, spark, sample_data):
        """Empty strings in PK columns should be treated as invalid."""
        # Row with product_id="" should be invalid
        pk_columns = ["product_id"]
        
        valid_condition = F.lit(True)
        for col_name in pk_columns:
            valid_condition = valid_condition & (
                F.col(col_name).isNotNull() & 
                (F.trim(F.col(col_name)) != "")
            )
        
        invalid_df = sample_data.filter(~valid_condition)
        # Rows: null product_id, empty product_id
        assert invalid_df.count() == 2


class TestNullifyEmptyStrings:
    """Tests for empty string to null conversion."""
    
    def test_empty_strings_become_null(self, spark, sample_data):
        """Empty strings should be converted to null."""
        # Apply nullify
        result_df = sample_data
        for col_name in result_df.columns:
            if result_df.schema[col_name].dataType == StringType():
                result_df = result_df.withColumn(
                    col_name,
                    F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.col(col_name))
                )
        
        # Check P004's location_id is now null
        p004 = result_df.filter(F.col("product_id") == "P004")
        assert p004.first()["location_id"] is None
    
    def test_non_empty_strings_unchanged(self, spark, sample_data):
        """Non-empty strings should remain unchanged."""
        result_df = sample_data
        for col_name in result_df.columns:
            if result_df.schema[col_name].dataType == StringType():
                result_df = result_df.withColumn(
                    col_name,
                    F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.col(col_name))
                )
        
        p001 = result_df.filter(F.col("product_id") == "P001")
        assert p001.first()["location_id"] == "L001"


class TestPeriodDerivation:
    """Tests for period column derivation logic."""
    
    def test_concat_period(self, spark):
        """Test period derivation with apply_concat=True."""
        data = [("2024-01-15",), ("2024-03-20",), ("2024-12-01",)]
        df = spark.createDataFrame(data, ["forecast_date"])
        
        result = df.withColumn(
            "period",
            F.concat_ws("-", F.substring(F.col("forecast_date"), 1, 7), F.lit("01"))
        )
        
        periods = [row["period"] for row in result.collect()]
        assert periods == ["2024-01-01", "2024-03-01", "2024-12-01"]
    
    def test_direct_period(self, spark):
        """Test period derivation with apply_concat=False."""
        data = [("2024-01-15",), ("2024-03-20",)]
        df = spark.createDataFrame(data, ["forecast_date"])
        
        result = df.withColumn("period", F.col("forecast_date"))
        
        periods = [row["period"] for row in result.collect()]
        assert periods == ["2024-01-15", "2024-03-20"]


class TestAggregation:
    """Tests for the aggregated audit logic."""
    
    def test_metric_aggregation(self, spark):
        """Test KPI metric aggregation with stack/transpose."""
        data = [
            ("file1.csv", 100.0, 5000.0),
            ("file1.csv", 200.0, 10000.0),
            ("file2.csv", 50.0, 2500.0),
        ]
        df = spark.createDataFrame(data, ["fl_nm", "forecast_qty", "revenue_amount"])
        
        # Aggregate
        agg_df = df.groupBy("fl_nm").agg(
            F.count(F.lit(1)).alias("no_of_records"),
            F.sum("forecast_qty").alias("forecast_qty"),
            F.sum("revenue_amount").alias("revenue_amount")
        )
        
        # Verify file1 aggregation
        file1 = agg_df.filter(F.col("fl_nm") == "file1.csv").first()
        assert file1["no_of_records"] == 2
        assert file1["forecast_qty"] == 300.0
        assert file1["revenue_amount"] == 15000.0
    
    def test_stack_transpose(self, spark):
        """Test stack/transpose of metric columns."""
        data = [("file1.csv", 2, 300.0, 15000.0)]
        df = spark.createDataFrame(
            data, ["fl_nm", "no_of_records", "forecast_qty", "revenue_amount"]
        )
        
        # Stack expression
        transposed = df.selectExpr(
            "fl_nm", "no_of_records",
            "stack(2, 'forecast_qty', forecast_qty, 'revenue_amount', revenue_amount) as (keyfigure, total_qty_amount)"
        )
        
        assert transposed.count() == 2
        rows = transposed.collect()
        kpis = {row["keyfigure"]: row["total_qty_amount"] for row in rows}
        assert kpis["forecast_qty"] == 300.0
        assert kpis["revenue_amount"] == 15000.0
