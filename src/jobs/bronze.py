"""Job da camada bronze: landing zone -> tabelas Delta com schema padronizado.

Le cada parquet individualmente e aplica cast explicito antes do union,
resolvendo o schema drift entre meses da fonte TLC (ver utils/schemas.py).
Adiciona metadados de ingestao (_source_file, _ingestion_ts) para
rastreabilidade e permitir auditoria por arquivo de origem.
"""

import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import table_name
from src.utils.schemas import standardize_schema


class BronzeJob:
    """Ingesta os arquivos da landing zone para tabelas Delta na bronze."""

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.landing_volume = config["landing_volume"]

    def run(self) -> None:
        """Processa cada tipo de taxi configurado e grava sua tabela bronze."""
        for taxi_type, columns in self.config["taxi_types"].items():
            df = self._load_taxi_type(
                taxi_type, columns["pickup_col"], columns["dropoff_col"]
            )
            target = table_name(self.config, "bronze", f"{taxi_type}_taxi_trips")
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(target)
            )
            count = self.spark.table(target).count()
            print(f"[bronze] {target}: {count:,} registros")

    def _load_taxi_type(
        self, taxi_type: str, pickup_col: str, dropoff_col: str
    ) -> DataFrame:
        """Le todos os parquets de um tipo de taxi, com cast por arquivo."""
        source_dir = os.path.join(self.landing_volume, taxi_type)
        files = sorted(
            os.path.join(source_dir, name)
            for name in os.listdir(source_dir)
            if name.endswith(".parquet")
        )
        if not files:
            raise ValueError(f"Nenhum parquet encontrado em {source_dir}")

        frames = []
        for path in files:
            df = standardize_schema(
                self.spark.read.parquet(path), pickup_col, dropoff_col
            )
            frames.append(
                df.withColumn("_source_file", F.lit(os.path.basename(path)))
                .withColumn("_ingestion_ts", F.current_timestamp())
            )
        return reduce(lambda left, right: left.unionByName(right), frames)
