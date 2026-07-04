# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Dado cru em Delta com metadados de ingestão
# MAGIC
# MAGIC A lógica está dividida entre `src/jobs/bronze.py` e `src/utils/schemas.py`.
# MAGIC
# MAGIC O ponto que mais me chamou atenção nessa etapa: os parquets de 2023 não têm
# MAGIC um schema estável entre os meses. `passenger_count`, por exemplo, aparece
# MAGIC como `int64` em alguns arquivos e `double` em outros. Se eu simplesmente
# MAGIC lesse a pasta inteira com `spark.read.parquet(pasta)`, o Spark ia inferir um
# MAGIC schema por conta própria e provavelmente ia dar problema no union entre os
# MAGIC meses. Por isso leio arquivo por arquivo e aplico um cast explícito para um
# MAGIC schema alvo antes de unir tudo — assim eu controlo o resultado em vez de
# MAGIC torcer para a coerção implícita funcionar.
# MAGIC
# MAGIC Também adiciono duas colunas de controle, `_source_file` e
# MAGIC `_ingestion_ts`. Isso não é exigido pelo enunciado, mas é barato de fazer e
# MAGIC me dá rastreabilidade: se um número parecer estranho lá na frente, dá pra
# MAGIC voltar e saber exatamente de qual arquivo e de qual momento de ingestão ele
# MAGIC veio.
# MAGIC
# MAGIC E por que a bronze não aplica nenhum filtro de qualidade? Porque essa
# MAGIC camada representa o dado como ele chegou — o "fato bruto". Se um dia eu
# MAGIC precisar mudar uma regra de qualidade, quero poder reprocessar a silver a
# MAGIC partir da bronze sem ter que baixar tudo de novo da fonte.

# COMMAND ----------

import sys, os
repo_root = os.path.abspath("..")
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.config import load_config
from src.jobs.bronze import BronzeJob

config = load_config()
BronzeJob(spark, config).run()
