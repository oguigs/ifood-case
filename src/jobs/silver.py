"""Job da camada silver: bronze -> camada de consumo + quarentena.

Fluxo:
1. Unifica yellow e green (renomeando tpep_/lpep_ para nomes canonicos).
2. Deduplica pelas colunas de negocio (a fonte TLC contem duplicatas).
3. Aplica as regras de qualidade (padrao de quarentena): registros aprovados
   vao para a tabela de consumo; reprovados vao para a quarentena com o
   motivo de cada falha, preservando auditabilidade.
4. Particiona a silver por (pickup_year, pickup_month), alinhado ao padrao
   de consulta das perguntas do case (agrupamento mensal e filtro de maio).
5. Publica a view yellow_taxi_trips com os nomes originais tpep_*,
   atendendo literalmente a exigencia do enunciado.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import table_name
from src.utils import data_quality as dq

BUSINESS_KEY = [
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "total_amount",
    "taxi_type",
]


class SilverJob:
    """Constroi a camada de consumo com regras de qualidade e quarentena."""

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config

    def run(self) -> None:
        unified = self._unify_taxi_types()
        deduplicated = unified.dropDuplicates(BUSINESS_KEY)

        rules = dq.build_quality_rules(
            min_pickup_date=self.config["quality"]["min_pickup_date"],
            max_pickup_date_exclusive=self.config["quality"][
                "max_pickup_date_exclusive"
            ],
            max_trip_duration_hours=self.config["quality"][
                "max_trip_duration_hours"
            ],
        )
        labeled = dq.apply_quality_rules(deduplicated, rules)

        # Nota: não usamos .cache()/.persist() aqui de propósito — o
        # Databricks Free Edition roda em compute serverless, que não
        # suporta PERSIST TABLE. O DataFrame `labeled` é recalculado uma
        # vez para cada escrita (silver e quarentena); para o volume deste
        # case (~16M linhas), o custo extra é irrelevante frente ao ganho
        # de rodar sem depender de compute clássico.
        passed, failed = dq.split_by_status(labeled)
        self._write_silver(passed)
        self._write_quarantine(failed)

        self._create_yellow_view()

    def _unify_taxi_types(self) -> DataFrame:
        """Le as tabelas bronze e as unifica com nomes canonicos de colunas."""
        frames = []
        for taxi_type, columns in self.config["taxi_types"].items():
            source = table_name(self.config, "bronze", f"{taxi_type}_taxi_trips")
            frames.append(
                self.spark.table(source)
                .withColumnRenamed(columns["pickup_col"], "pickup_datetime")
                .withColumnRenamed(columns["dropoff_col"], "dropoff_datetime")
                .withColumn("taxi_type", F.lit(taxi_type))
                .select(*BUSINESS_KEY)
            )
        unified = frames[0]
        for frame in frames[1:]:
            unified = unified.unionByName(frame)
        return unified

    def _write_silver(self, df: DataFrame) -> None:
        """Grava os registros aprovados, particionados por ano/mes de embarque."""
        target = table_name(self.config, "silver", "taxi_trips")
        enriched = df.withColumn(
            "pickup_year", F.year("pickup_datetime")
        ).withColumn("pickup_month", F.month("pickup_datetime"))
        (
            enriched.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("pickup_year", "pickup_month")
            .saveAsTable(target)
        )
        print(f"[silver] {target}: {self.spark.table(target).count():,} registros")

    def _write_quarantine(self, df: DataFrame) -> None:
        """Grava os registros reprovados com os motivos de falha."""
        target = table_name(self.config, "silver", "taxi_trips_quarantine")
        quarantine = df.withColumn(
            dq.DQ_FAILURES_COL, F.concat_ws(",", F.col(dq.DQ_FAILURES_COL))
        ).withColumn("_quarantined_at", F.current_timestamp())
        (
            quarantine.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target)
        )
        total = self.spark.table(target).count()
        print(f"[quarantine] {target}: {total:,} registros reprovados")
        if total > 0:
            print("[quarantine] falhas por motivo:")
            (
                self.spark.table(target)
                .groupBy(dq.DQ_FAILURES_COL)
                .count()
                .orderBy(F.desc("count"))
                .show(truncate=False)
            )

    def _create_yellow_view(self) -> None:
        """Publica a view de consumo com os nomes originais tpep_* (enunciado)."""
        silver = table_name(self.config, "silver", "taxi_trips")
        view = table_name(self.config, "silver", "yellow_taxi_trips")
        self.spark.sql(
            f"""
            CREATE OR REPLACE VIEW {view} AS
            SELECT
              VendorID,
              passenger_count,
              total_amount,
              pickup_datetime  AS tpep_pickup_datetime,
              dropoff_datetime AS tpep_dropoff_datetime
            FROM {silver}
            WHERE taxi_type = 'yellow'
            """
        )
        print(f"[silver] view {view} publicada")
