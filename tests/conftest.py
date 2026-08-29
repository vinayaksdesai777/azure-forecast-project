"""
Shared pytest fixtures.

A local SparkSession is not always startable. On Windows the launcher scripts
under SPARK_HOME/bin do not quote paths, so an install path containing a space
fails with "The system cannot find the file C:\\Users\\First". That is an
environment fault, not a code fault, so the Spark-backed tests skip with a
clear reason rather than reporting failures that say nothing about the
pipeline.

Where they do run: any Databricks cluster, CI on Linux, or a local checkout on
a path with no spaces.
"""

import os
import sys

import pytest


def _spark_or_none():
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None, "pyspark is not installed"

    # Point the workers at this interpreter; without it they inherit whatever
    # python is first on PATH, which may not have pyspark.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    try:
        session = (
            SparkSession.builder.appName("pipeline-tests")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        # Force the JVM to actually do something: builder.getOrCreate() can
        # return before the gateway has failed.
        session.createDataFrame([(1,)], "probe int").count()
        return session, None
    except Exception as exc:  # JVM gateway failures surface in many shapes
        return None, f"could not start a local SparkSession: {type(exc).__name__}: {exc}"


@pytest.fixture(scope="session")
def spark():
    session, reason = _spark_or_none()
    if session is None:
        pytest.skip(reason)
    yield session
    session.stop()
