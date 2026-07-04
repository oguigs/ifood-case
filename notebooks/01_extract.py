# Databricks notebook source
# MAGIC %md
# MAGIC # Extração — NYC TLC → Landing Zone
# MAGIC
# MAGIC Download dos parquets oficiais (Jan–Mai/2023) para o Volume, preservando os
# MAGIC arquivos originais intactos. Toda a lógica vive em `src/jobs/extract.py`;
# MAGIC este notebook apenas orquestra.
# MAGIC
# MAGIC **Por que yellow E green:** a pergunta 2 do case pede "todos os táxis da
# MAGIC frota". Incluir os green taxis (colunas `lpep_*`) atende ao enunciado de
# MAGIC forma literal — a unificação de nomes acontece na silver.
# MAGIC
# MAGIC **Por que retry com backoff e escrita em `.tmp`:** o CDN da TLC pode falhar
# MAGIC transitoriamente; o retry evita rerun manual e o arquivo temporário renomeado
# MAGIC ao final garante que um download interrompido nunca deixe um parquet
# MAGIC corrompido na landing zone (escrita atômica).

# COMMAND ----------

import sys, os
repo_root = os.path.abspath("..")
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.config import load_config
from src.jobs.extract import ExtractJob

config = load_config()
ExtractJob(config).run()

