"""Ponto de entrada do pipeline.

Uso (num notebook Databricks ou terminal com Spark disponivel):

    python -m src.main extract
    python -m src.main bronze
    python -m src.main silver
    python -m src.main all
"""

import argparse

from pyspark.sql import SparkSession

from src.config import load_config
from src.jobs.bronze import BronzeJob
from src.jobs.extract import ExtractJob
from src.jobs.silver import SilverJob

TASKS = ("extract", "bronze", "silver", "all")


def build_spark(app_name: str) -> SparkSession:
    """Obtem a SparkSession (no Databricks, reutiliza a sessao gerenciada)."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def run_task(task: str, config: dict) -> None:
    """Executa uma tarefa do pipeline (ou todas, em ordem)."""
    if task in ("extract", "all"):
        ExtractJob(config).run()
    if task in ("bronze", "all"):
        BronzeJob(build_spark("ifood-case-bronze"), config).run()
    if task in ("silver", "all"):
        SilverJob(build_spark("ifood-case-silver"), config).run()


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline do case iFood")
    parser.add_argument("task", choices=TASKS, help="Etapa a executar")
    args = parser.parse_args()
    run_task(args.task, load_config())


if __name__ == "__main__":
    main()
