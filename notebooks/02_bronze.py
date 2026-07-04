# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Dado cru em Delta com metadados de ingestão
# MAGIC
# MAGIC Lógica em `src/jobs/bronze.py` + `src/utils/schemas.py`.
# MAGIC
# MAGIC **Por que ler arquivo a arquivo com cast explícito (e não `spark.read` na
# MAGIC pasta inteira):** os parquets de 2023 têm **schema drift entre meses** —
# MAGIC `passenger_count` aparece como int64 em alguns arquivos e double em outros.
# MAGIC O cast por arquivo para um schema alvo torna o union determinístico, em vez
# MAGIC de depender de coerção implícita de tipos.
# MAGIC
# MAGIC **Por que `_source_file` e `_ingestion_ts`:** rastreabilidade — qualquer
# MAGIC registro da bronze pode ser auditado até o arquivo de origem e o momento da
# MAGIC ingestão, pré-requisito de um data lake auditável.
# MAGIC
# MAGIC **Por que a bronze não filtra nada:** esta camada é o fato imutável. Manter
# MAGIC o dado cru permite reprocessar a silver com novas regras sem voltar à fonte.

# COMMAND ----------

import sys, os
repo_root = os.path.abspath("..")
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.config import load_config
from src.jobs.bronze import BronzeJob

config = load_config()
BronzeJob(spark, config).run()

