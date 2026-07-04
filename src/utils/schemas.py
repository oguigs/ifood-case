"""Padronizacao de schema dos arquivos da NYC TLC.

Os parquets mensais de 2023 apresentam schema drift: passenger_count aparece
como int64 em alguns meses e double em outros, e VendorID tambem varia.
A leitura arquivo a arquivo com cast explicito para um schema alvo garante
que o union entre meses e entre tipos de taxi (yellow/green) seja deterministico,
em vez de depender de coercao implicita de tipos.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

REQUIRED_COLUMNS = [
    "VendorID",
    "passenger_count",
    "total_amount",
]


def standardize_schema(df: DataFrame, pickup_col: str, dropoff_col: str) -> DataFrame:
    """Aplica o schema alvo as colunas obrigatorias do case.

    :param df: DataFrame lido de um unico arquivo parquet da TLC.
    :param pickup_col: nome original da coluna de embarque (tpep_/lpep_).
    :param dropoff_col: nome original da coluna de desembarque (tpep_/lpep_).
    :return: DataFrame apenas com as colunas obrigatorias, tipadas.
    """
    return (
        df.withColumn("VendorID", F.col("VendorID").cast(IntegerType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
        .withColumn(pickup_col, F.col(pickup_col).cast(TimestampType()))
        .withColumn(dropoff_col, F.col(dropoff_col).cast(TimestampType()))
        .select(*REQUIRED_COLUMNS, pickup_col, dropoff_col)
    )
