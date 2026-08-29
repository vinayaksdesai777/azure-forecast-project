"""
Regression tests against the real pipeline utilities.

test_data_quality.py reimplements its assertions inline — it builds its own
null-check expression rather than calling NullPKCheck — so it passes even when
the shipped code is broken. These tests import databricks/utilities directly, so
a regression in the pipeline fails the suite.

Every case here corresponds to a defect that actually reached a running load.
The comment on each names it, so a future change that reintroduces one fails
with an explanation rather than a bare assertion error.
"""

import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark not available in this environment")


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "databricks"))

from utilities.dq_checks import NullPKCheck, DedupCheck, DomainCheck  # noqa: E402
from utilities.transformations import build_surrogate_key, resolve_dim_sk  # noqa: E402


# ── DQ checks ────────────────────────────────────────────────────────────────

class TestNullPKCheck:
    def test_null_pk_is_routed_out(self, spark):
        """The seeds plant ~1% NULL product_id as dirty data; those rows must
        leave the main flow rather than reaching the SCD2 merge."""
        df = spark.createDataFrame(
            [("P1", "L1", "2025-01-01"), (None, "L2", "2025-01-01"), ("P3", "L3", "2025-01-01")],
            "product_id string, location_id string, forecast_date string",
        )
        valid, invalid, result = NullPKCheck(["product_id", "location_id", "forecast_date"]).run(df)

        assert valid.count() == 2
        assert invalid.count() == 1
        assert result["records_failed"] == 1

    def test_empty_string_counts_as_null(self, spark):
        """A Parquet column read as string yields "" where the source had NULL,
        so an isNotNull() check alone lets the row through."""
        df = spark.createDataFrame(
            [("P1", "L1", "2025-01-01"), ("", "L2", "2025-01-01")],
            "product_id string, location_id string, forecast_date string",
        )
        valid, invalid, _ = NullPKCheck(["product_id", "location_id", "forecast_date"]).run(df)

        assert valid.count() == 1
        assert invalid.count() == 1


class TestDedupCheck:
    def test_one_row_survives_per_key(self, spark):
        """Delta MERGE raises if two source rows match one target row, so the
        batch must be deduplicated before the merge, not after."""
        df = spark.createDataFrame(
            [("P1", "L1", "2025-01-01", "10", "2025-01-01 01:00:00"),
             ("P1", "L1", "2025-01-01", "20", "2025-01-01 02:00:00"),
             ("P2", "L2", "2025-01-01", "30", "2025-01-01 01:00:00")],
            "product_id string, location_id string, forecast_date string, qty string, "
            "_ingestion_ts string",
        )
        valid, invalid, result = DedupCheck(["product_id", "location_id", "forecast_date"]).run(df)

        assert valid.count() == 2
        assert invalid.count() == 1
        assert result["records_failed"] == 1


class TestDomainCheck:
    def test_invalid_values_are_flagged_not_dropped(self, spark):
        """Domain checks are informational: the seeds inject HARDWARE / UNKNOWN
        / MISC deliberately and those rows must still reach Silver, counted."""
        df = spark.createDataFrame(
            [("SERVER",), ("HARDWARE",), ("STORAGE",)], "category string"
        )
        valid, invalid, result = DomainCheck("category", {"SERVER", "STORAGE"}).run(df)

        assert result["records_failed"] == 1
        assert valid.count() == 3, "DomainCheck must not remove rows from the main flow"


# ── Surrogate keys ───────────────────────────────────────────────────────────

class TestBuildSurrogateKey:
    def test_versions_created_same_day_get_distinct_keys(self, spark):
        """effective_from has day granularity. Hashing only (natural_key,
        effective_from) gave two versions of one product the same product_sk
        when several subjects loaded in one session, and the star view then
        matched each fact against every colliding row — 4,030,130 rows against
        1,972,706 facts."""
        df = spark.createDataFrame(
            [("HPE-PROD-0001", "SERVER", "PROLIANT", "2026-08-16"),
             ("HPE-PROD-0001", "HARDWARE", "LEGACY", "2026-08-16")],
            "product_id string, category string, sub_category string, effective_from string",
        )
        out = build_surrogate_key(df, "product_id", tracked_cols=["category", "sub_category"])

        assert out.select("product_sk").distinct().count() == 2

    def test_key_is_deterministic_across_rebuilds(self, spark):
        """The key is a hash rather than an IDENTITY precisely so a dimension
        rebuild leaves existing fact rows still pointing at the right member."""
        rows = [("HPE-PROD-0001", "SERVER", "PROLIANT", "2026-08-16")]
        schema = "product_id string, category string, sub_category string, effective_from string"
        cols = ["category", "sub_category"]

        first = build_surrogate_key(spark.createDataFrame(rows, schema), "product_id", tracked_cols=cols)
        second = build_surrogate_key(spark.createDataFrame(rows, schema), "product_id", tracked_cols=cols)

        assert first.first()["product_sk"] == second.first()["product_sk"]


class TestResolveDimSk:
    def test_only_the_active_version_is_joined(self, spark):
        """A member with an expired and an active version must match once. This
        filter is what keeps every measure from doubling."""
        dim = spark.createDataFrame(
            [("sk_old", "HPE-PROD-0001", False), ("sk_new", "HPE-PROD-0001", True)],
            "product_sk string, product_id string, is_active boolean",
        )
        fact = spark.createDataFrame([("HPE-PROD-0001", 100.0)], "product_id string, qty double")

        out = resolve_dim_sk(fact, dim, "product_id", "product_sk")

        assert out.count() == 1
        assert out.first()["product_sk"] == "sk_new"

    def test_unmatched_fact_keeps_a_sentinel_key(self, spark):
        """Fact rows survive a missing dimension member rather than being
        silently dropped by an inner join."""
        dim = spark.createDataFrame(
            [("sk1", "HPE-PROD-0001", True)],
            "product_sk string, product_id string, is_active boolean",
        )
        fact = spark.createDataFrame([("HPE-PROD-9999", 100.0)], "product_id string, qty double")

        out = resolve_dim_sk(fact, dim, "product_id", "product_sk")

        assert out.count() == 1
        assert out.first()["product_sk"] == "UNKNOWN"
