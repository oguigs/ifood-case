"""Testes unitarios das regras de qualidade (padrao de quarentena).

Cobre um cenario por regra, mais dois casos de premissa documentada:
- passenger_count NULL deve PASSAR (preservado para analises de receita);
- pickup fora do range Jan-Mai/2023 deve FALHAR (a fonte TLC contem
  registros residuais de outros periodos).
"""

from datetime import datetime, timedelta

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from src.utils.data_quality import (
    STATUS_FAIL,
    STATUS_PASS,
    apply_quality_rules,
    build_quality_rules,
    split_by_status,
)

SCHEMA = StructType(
    [
        StructField("total_amount", DoubleType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
    ]
)

BASE_PICKUP = datetime(2023, 3, 15, 14, 0, 0)


def make_rules():
    return build_quality_rules(
        min_pickup_date="2023-01-01",
        max_pickup_date_exclusive="2023-06-01",
        max_trip_duration_hours=24,
    )


CASES = [
    # (id, total_amount, passenger_count, pickup, dropoff, status_esperado, motivo)
    (
        "valid_trip",
        25.5, 2, BASE_PICKUP, BASE_PICKUP + timedelta(minutes=20),
        STATUS_PASS, None,
    ),
    (
        "null_passenger_count_passes",
        25.5, None, BASE_PICKUP, BASE_PICKUP + timedelta(minutes=20),
        STATUS_PASS, None,
    ),
    (
        "null_total_amount",
        None, 1, BASE_PICKUP, BASE_PICKUP + timedelta(minutes=20),
        STATUS_FAIL, "null_total_amount",
    ),
    (
        "non_positive_total_amount",
        -7.0, 1, BASE_PICKUP, BASE_PICKUP + timedelta(minutes=20),
        STATUS_FAIL, "non_positive_total_amount",
    ),
    (
        "non_positive_passenger_count",
        25.5, 0, BASE_PICKUP, BASE_PICKUP + timedelta(minutes=20),
        STATUS_FAIL, "non_positive_passenger_count",
    ),
    (
        "null_pickup_datetime",
        25.5, 1, None, BASE_PICKUP + timedelta(minutes=20),
        STATUS_FAIL, "null_pickup_datetime",
    ),
    (
        "null_dropoff_datetime",
        25.5, 1, BASE_PICKUP, None,
        STATUS_FAIL, "null_dropoff_datetime",
    ),
    (
        "non_positive_trip_duration",
        25.5, 1, BASE_PICKUP, BASE_PICKUP - timedelta(minutes=5),
        STATUS_FAIL, "non_positive_trip_duration",
    ),
    (
        "trip_duration_too_long",
        25.5, 1, BASE_PICKUP, BASE_PICKUP + timedelta(days=2),
        STATUS_FAIL, "trip_duration_too_long",
    ),
    (
        "pickup_before_range",
        25.5, 1, datetime(2022, 12, 31, 23, 0), datetime(2022, 12, 31, 23, 30),
        STATUS_FAIL, "pickup_before_range",
    ),
    (
        "pickup_after_range",
        25.5, 1, datetime(2023, 7, 1, 10, 0), datetime(2023, 7, 1, 10, 30),
        STATUS_FAIL, "pickup_after_range",
    ),
]


@pytest.mark.parametrize(
    "case_id,total,passengers,pickup,dropoff,expected_status,expected_reason",
    CASES,
    ids=[case[0] for case in CASES],
)
def test_quality_rules_by_case(
    spark, case_id, total, passengers, pickup, dropoff, expected_status, expected_reason
):
    df = spark.createDataFrame([(total, passengers, pickup, dropoff)], SCHEMA)
    result = apply_quality_rules(df, make_rules()).collect()[0]

    assert result["dq_status"] == expected_status
    if expected_reason is None:
        assert result["dq_failures"] == []
    else:
        assert expected_reason in result["dq_failures"]


def test_record_can_accumulate_multiple_failures(spark):
    """Um registro pode falhar em mais de uma regra ao mesmo tempo."""
    df = spark.createDataFrame(
        [(-3.0, 0, BASE_PICKUP, BASE_PICKUP - timedelta(minutes=10))], SCHEMA
    )
    result = apply_quality_rules(df, make_rules()).collect()[0]

    assert result["dq_status"] == STATUS_FAIL
    assert set(result["dq_failures"]) >= {
        "non_positive_total_amount",
        "non_positive_passenger_count",
        "non_positive_trip_duration",
    }


def test_split_by_status_separates_and_cleans_columns(spark):
    """split_by_status separa PASS/FAIL e remove colunas de DQ dos aprovados."""
    rows = [
        (25.5, 2, BASE_PICKUP, BASE_PICKUP + timedelta(minutes=20)),
        (-7.0, 1, BASE_PICKUP, BASE_PICKUP + timedelta(minutes=20)),
    ]
    labeled = apply_quality_rules(spark.createDataFrame(rows, SCHEMA), make_rules())
    passed, failed = split_by_status(labeled)

    assert passed.count() == 1
    assert failed.count() == 1
    assert "dq_status" not in passed.columns
    assert "dq_failures" not in passed.columns
    assert "dq_failures" in failed.columns
