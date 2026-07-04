# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Camada de consumo com quarentena de qualidade
# MAGIC
# MAGIC Unifica yellow + green, deduplica, aplica as regras de qualidade
# MAGIC (`src/utils/data_quality.py`) e separa aprovados (consumo) de reprovados
# MAGIC (quarentena). Lógica em `src/jobs/silver.py`.
# MAGIC
# MAGIC **Por que quarentena em vez de descartar:** registros reprovados são
# MAGIC preservados em `taxi_trips_quarantine` com o motivo exato de cada falha.
# MAGIC Isso dá auditabilidade (quantos registros por regra), permite monitorar
# MAGIC regressões na fonte ao longo do tempo e viabiliza reprocessamento — em
# MAGIC produção, dado descartado silenciosamente é dado irrecuperável.
# MAGIC
# MAGIC **Por que dedup ANTES das regras de DQ:** duplicatas são um problema de
# MAGIC identidade do registro, não de qualidade do conteúdo — um registro duplicado
# MAGIC válido não deve poluir a quarentena.
# MAGIC
# MAGIC **Premissa documentada — `passenger_count` NULL passa, zero não:** NULL
# MAGIC significa campo não preenchido pelo taxímetro (comum na fonte TLC) e não
# MAGIC invalida a corrida para análises de receita; zero é uma contagem inválida.
# MAGIC Análises de passageiros (Q2) filtram NULL na própria query.
# MAGIC
# MAGIC **Por que particionar por `pickup_year, pickup_month`:** alinhado ao padrão
# MAGIC de consulta do case — a Q1 agrega por mês e a Q2 filtra maio, ambas com
# MAGIC partition pruning direto. `taxi_type` (2 valores) daria partições grandes
# MAGIC demais e sem ganho para essas queries.

# COMMAND ----------

import sys, os
repo_root = os.path.abspath("..")
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.config import load_config
from src.jobs.silver import SilverJob

config = load_config()
SilverJob(spark, config).run()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação — volumetria por camada
# MAGIC
# MAGIC Conferência de sanidade: a soma bronze deve equivaler a silver + quarentena
# MAGIC + duplicatas removidas. Esses números alimentam a tabela de premissas do
# MAGIC README.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze_yellow' AS tabela, COUNT(*) AS total FROM ifood_case.bronze.yellow_taxi_trips
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_green', COUNT(*) FROM ifood_case.bronze.green_taxi_trips
# MAGIC UNION ALL
# MAGIC SELECT 'silver_total', COUNT(*) FROM ifood_case.silver.taxi_trips
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine', COUNT(*) FROM ifood_case.silver.taxi_trips_quarantine
# MAGIC UNION ALL
# MAGIC SELECT 'silver_yellow_view', COUNT(*) FROM ifood_case.silver.yellow_taxi_trips;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auditoria da quarentena — falhas por motivo
# MAGIC
# MAGIC Cada regra de qualidade com sua contagem exata de registros capturados.
# MAGIC É a evidência quantitativa que justifica cada premissa adotada — e, em
# MAGIC produção, a série temporal dessas contagens é o sinal de alerta para
# MAGIC regressões de qualidade na fonte.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dq_failures, COUNT(*) AS qtd
# MAGIC FROM ifood_case.silver.taxi_trips_quarantine
# MAGIC GROUP BY dq_failures
# MAGIC ORDER BY qtd DESC;

