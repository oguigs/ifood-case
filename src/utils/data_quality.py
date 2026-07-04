"""Regras de qualidade de dados aplicadas na transicao bronze -> silver.

Padrao de quarentena: em vez de descartar registros invalidos, cada registro
recebe um status ('PASS'/'FAIL') e a lista dos motivos de falha. Registros
reprovados sao preservados em uma tabela de quarentena para auditoria e
possivel reprocessamento.

As funcoes deste modulo sao puras (DataFrame -> DataFrame), sem dependencia
de Databricks, o que permite testa-las localmente com pytest.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

DQ_STATUS_COL = "dq_status"
DQ_FAILURES_COL = "dq_failures"
STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"


def build_quality_rules(
    min_pickup_date: str,
    max_pickup_date_exclusive: str,
    max_trip_duration_hours: int,
) -> list[tuple[Column, str]]:
    """Constroi a lista de regras de qualidade como pares (condicao_de_falha, motivo).

    Premissas documentadas:
    - total_amount <= 0: estornos/ajustes administrativos, nao corridas efetivas.
    - passenger_count <= 0: contagem invalida. NULL e tolerado (campo nao
      preenchido pelo taximetro e comum na fonte TLC e nao invalida a corrida
      para analises de receita; analises de passageiros filtram NULL na query).
    - Datas nulas ou duracao nao positiva: erro de registro na fonte.
    - Duracao > max_trip_duration_hours: outlier fisicamente implausivel.
    - pickup fora do range solicitado: os arquivos mensais da TLC contem
      registros residuais de outros periodos (erro de relogio na fonte).
    """
    duration_seconds = F.unix_timestamp(F.col("dropoff_datetime")) - F.unix_timestamp(
        F.col("pickup_datetime")
    )
    max_duration_seconds = max_trip_duration_hours * 3600

    return [
        (F.col("total_amount").isNull(), "null_total_amount"),
        (F.col("total_amount") <= 0, "non_positive_total_amount"),
        (F.col("passenger_count") <= 0, "non_positive_passenger_count"),
        (F.col("pickup_datetime").isNull(), "null_pickup_datetime"),
        (F.col("dropoff_datetime").isNull(), "null_dropoff_datetime"),
        (
            F.col("dropoff_datetime") <= F.col("pickup_datetime"),
            "non_positive_trip_duration",
        ),
        (duration_seconds > max_duration_seconds, "trip_duration_too_long"),
        (F.col("pickup_datetime") < F.lit(min_pickup_date), "pickup_before_range"),
        (
            F.col("pickup_datetime") >= F.lit(max_pickup_date_exclusive),
            "pickup_after_range",
        ),
    ]


def apply_quality_rules(
    df: DataFrame, rules: list[tuple[Column, str]]
) -> DataFrame:
    """Avalia todas as regras e anexa as colunas de status de qualidade.

    :param df: DataFrame com as colunas de negocio padronizadas.
    :param rules: lista de pares (condicao_de_falha, motivo), tipicamente
        construida por build_quality_rules().
    :return: DataFrame original + colunas dq_failures (array<string>) e
        dq_status ('PASS' quando o array esta vazio, 'FAIL' caso contrario).
    """
    failure_flags = F.array(
        *[F.when(condition, F.lit(reason)) for condition, reason in rules]
    )

    return (
        df.withColumn(
            DQ_FAILURES_COL,
            F.filter(failure_flags, lambda flag: flag.isNotNull()),
        )
        .withColumn(
            DQ_STATUS_COL,
            F.when(F.size(F.col(DQ_FAILURES_COL)) == 0, F.lit(STATUS_PASS)).otherwise(
                F.lit(STATUS_FAIL)
            ),
        )
    )


def split_by_status(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separa o DataFrame rotulado em (aprovados, reprovados).

    Os aprovados saem sem as colunas de controle de qualidade; os reprovados
    as mantem, para que a quarentena registre o motivo de cada falha.
    """
    passed = df.filter(F.col(DQ_STATUS_COL) == STATUS_PASS).drop(
        DQ_STATUS_COL, DQ_FAILURES_COL
    )
    failed = df.filter(F.col(DQ_STATUS_COL) == STATUS_FAIL)
    return passed, failed
