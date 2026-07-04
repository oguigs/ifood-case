# Databricks notebook source
# MAGIC %md
# MAGIC # Pergunta 2 — Média de passageiros por hora do dia em maio (todos os táxis)
# MAGIC
# MAGIC "Todos os táxis da frota" = yellow + green, unificados na silver com a
# MAGIC coluna `taxi_type`. O filtro de maio usa as colunas de partição
# MAGIC (partition pruning).
# MAGIC
# MAGIC **Premissa:** registros com `passenger_count` NULL são excluídos apenas
# MAGIC desta análise (campo não preenchido não é evidência de zero passageiros);
# MAGIC eles permanecem na silver para análises de receita.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   HOUR(pickup_datetime)          AS hora_do_dia,
# MAGIC   ROUND(AVG(passenger_count), 2) AS media_passageiros,
# MAGIC   COUNT(*)                       AS qtd_corridas
# MAGIC FROM ifood_case.silver.taxi_trips
# MAGIC WHERE pickup_year = 2023
# MAGIC   AND pickup_month = 5
# MAGIC   AND passenger_count IS NOT NULL
# MAGIC   AND passenger_count > 0
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de negócio
# MAGIC
# MAGIC A média é mais alta na madrugada (0h–3h, ~1,45) e à noite (20h–23h) —
# MAGIC deslocamentos em grupo após eventos sociais — e mínima às 6h (~1,26),
# MAGIC horário de corridas individuais para o trabalho. No horário comercial
# MAGIC (7h–19h) mantém-se estável entre 1,35 e 1,40.

