"""Fixtures compartilhadas dos testes (SparkSession local, sem Databricks)."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """SparkSession local minima para testes unitarios."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ifood-case-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
