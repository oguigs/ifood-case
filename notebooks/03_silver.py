# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Camada de consumo com quarentena de qualidade
# MAGIC
# MAGIC Aqui acontece a parte mais densa do pipeline: eu unifico yellow e green,
# MAGIC removo duplicatas, aplico as regras de qualidade
# MAGIC (`src/utils/data_quality.py`) e separo o que passou do que não passou. A
# MAGIC lógica completa mora em `src/jobs/silver.py`.
# MAGIC
# MAGIC Uma escolha que vale explicar de cara: quando um registro reprova numa
# MAGIC regra, eu não descarto ele — mando pra uma tabela de quarentena
# MAGIC (`taxi_trips_quarantine`) junto com o motivo exato da reprovação. Descartar
# MAGIC silenciosamente é fácil, mas em produção isso significa perder a chance de
# MAGIC auditar quantos registros cada regra está pegando, ou de reprocessar depois
# MAGIC se a regra mudar. Prefiro pagar um pouco mais de storage e manter tudo
# MAGIC rastreável.
# MAGIC
# MAGIC A deduplicação acontece antes da checagem de qualidade, de propósito.
# MAGIC Duplicata é um problema de identidade do registro (o mesmo evento aparece
# MAGIC duas vezes), não um problema do conteúdo em si — não faz sentido um
# MAGIC registro válido duplicado ir parar na quarentena como se fosse erro de
# MAGIC qualidade.
# MAGIC
# MAGIC Também documentei uma premissa que pode gerar dúvida: `passenger_count`
# MAGIC nulo passa, mas zero não. Nulo geralmente significa que o taxímetro não
# MAGIC preencheu o campo — isso é comum na fonte da TLC e não invalida a corrida
# MAGIC para quem quer analisar receita. Zero, por outro lado, é uma contagem que
# MAGIC não faz sentido físico. Quem for analisar passageiros (como na pergunta 2)
# MAGIC filtra os nulos direto na query.
# MAGIC
# MAGIC Por fim, particiono a tabela por `pickup_year` e `pickup_month` em vez de
# MAGIC `taxi_type`. Fiz essa escolha pensando no padrão de consulta do próprio
# MAGIC case: a pergunta 1 agrupa por mês e a pergunta 2 filtra só maio — as duas
# MAGIC se beneficiam de partition pruning dessa forma. Particionar por
# MAGIC `taxi_type`, com só 2 valores possíveis, não ajudaria em nada aqui.

# COMMAND ----------

import sys, os
repo_root = os.path.abspath("..")
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.config import load_config
from src.jobs.silver import SilverJob

config = load_config()
SilverJob(spark, config).run()  # Avoid caching in serverless environment if cache() is called inside

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação — volumetria por camada
# MAGIC
# MAGIC Antes de seguir, vale conferir se as contas fecham: a soma da bronze
# MAGIC (yellow + green) deve bater com silver + quarentena + o que foi removido
# MAGIC na deduplicação. Os números que saem daqui vão direto pra tabela de
# MAGIC premissas do README.

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
# MAGIC Essa query mostra quantos registros cada regra pegou, individualmente ou
# MAGIC combinada com outras. É o jeito mais direto de defender cada premissa que
# MAGIC documentei acima com número, não só com argumento — e, num cenário real de
# MAGIC produção, acompanhar essa distribuição ao longo do tempo é justamente como
# MAGIC se percebe se a qualidade da fonte está piorando.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dq_failures, COUNT(*) AS qtd
# MAGIC FROM ifood_case.silver.taxi_trips_quarantine
# MAGIC GROUP BY dq_failures
# MAGIC ORDER BY qtd DESC;
