"""Testes do modulo de padronizacao de schema (schema drift da fonte TLC)."""

from datetime import datetime

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.utils.schemas import standardize_schema


def test_standardize_schema_resolves_type_drift(spark):
    """Arquivos com passenger_count double e VendorID long convergem ao alvo."""
    drifted_schema = StructType(
        [
            StructField("VendorID", LongType(), True),
            StructField("passenger_count", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("tpep_pickup_datetime", StringType(), True),
            StructField("tpep_dropoff_datetime", StringType(), True),
            StructField("extra_column_ignored", StringType(), True),
        ]
    )
    df = spark.createDataFrame(
        [(2, 1.0, 25.5, "2023-03-15 14:00:00", "2023-03-15 14:20:00", "x")],
        drifted_schema,
    )

    result = standardize_schema(
        df, "tpep_pickup_datetime", "tpep_dropoff_datetime"
    )

    types = dict(result.dtypes)
    assert types["VendorID"] == "int"
    assert types["passenger_count"] == "int"
    assert types["total_amount"] == "double"
    assert types["tpep_pickup_datetime"] == "timestamp"
    assert types["tpep_dropoff_datetime"] == "timestamp"
    assert "extra_column_ignored" not in result.columns

    row = result.collect()[0]
    assert row["passenger_count"] == 1
    assert row["tpep_pickup_datetime"] == datetime(2023, 3, 15, 14, 0, 0)
