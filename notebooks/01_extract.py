# Databricks notebook source
# MAGIC %md
# MAGIC # Extração — NYC TLC → Landing Zone
# MAGIC
# MAGIC Aqui a gente baixa os parquets oficiais de janeiro a maio de 2023 e joga no
# MAGIC volume, sem mexer em nada — os arquivos ficam exatamente como vieram da
# MAGIC fonte. Toda a lógica está em `src/jobs/extract.py`; este notebook só chama
# MAGIC a classe e roda.
# MAGIC
# MAGIC Uma decisão que vale explicar: baixei yellow e green, não só yellow. A
# MAGIC pergunta 2 do case pede a média considerando "todos os táxis da frota",
# MAGIC então deixar o green de fora seria responder uma pergunta diferente da que
# MAGIC foi feita. A unificação de nomes de coluna (`lpep_*` vs `tpep_*`) fica pra
# MAGIC frente, na camada silver.
# MAGIC
# MAGIC Também coloquei retry com backoff no download. O CDN da TLC falha de vez em
# MAGIC quando por instabilidade de rede, e sem isso eu ia ter que ficar rodando o
# MAGIC notebook manualmente até dar certo. A escrita passa primeiro por um arquivo
# MAGIC `.tmp` que só é renomeado no final — assim, se cair no meio do download, não
# MAGIC sobra um parquet corrompido na landing zone.

# COMMAND ----------

import sys, os
repo_root = os.path.abspath("..")
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.config import load_config
from src.jobs.extract import ExtractJob

config = load_config()
ExtractJob(config).run()
