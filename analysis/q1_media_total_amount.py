# Databricks notebook source
# MAGIC %md
# MAGIC # Pergunta 1 — Média de `total_amount` recebido em um mês (yellow taxis)
# MAGIC
# MAGIC O enunciado admite duas leituras, e ambas são respondidas:
# MAGIC
# MAGIC 1. **Média por corrida, mês a mês** (leitura principal): valor médio de uma
# MAGIC    corrida em cada mês do período.
# MAGIC 2. **Receita média mensal da frota** (leitura alternativa): soma do mês,
# MAGIC    tirada a média entre os 5 meses.
# MAGIC
# MAGIC Apresentar as duas explicita a ambiguidade em vez de escondê-la — a escolha
# MAGIC da leitura correta depende da pergunta de negócio real (ticket médio vs
# MAGIC faturamento médio).
# MAGIC
# MAGIC A query usa as colunas de partição (`pickup_year`, `pickup_month`) para
# MAGIC aproveitar partition pruning.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   pickup_year,
# MAGIC   pickup_month,
# MAGIC   ROUND(AVG(total_amount), 2) AS media_total_amount,
# MAGIC   COUNT(*)                    AS qtd_corridas
# MAGIC FROM ifood_case.silver.taxi_trips
# MAGIC WHERE taxi_type = 'yellow'
# MAGIC GROUP BY pickup_year, pickup_month
# MAGIC ORDER BY pickup_year, pickup_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ROUND(AVG(receita_mensal), 2) AS receita_media_por_mes
# MAGIC FROM (
# MAGIC   SELECT pickup_year, pickup_month, SUM(total_amount) AS receita_mensal
# MAGIC   FROM ifood_case.silver.taxi_trips
# MAGIC   WHERE taxi_type = 'yellow'
# MAGIC   GROUP BY pickup_year, pickup_month
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resposta
# MAGIC
# MAGIC A média de `total_amount` por corrida variou de US$ [X] ([mês]) a US$ [Y]
# MAGIC ([mês]) entre janeiro e maio de 2023, com [tendência observada].
# MAGIC Alternativamente, considerando a receita total agregada por mês, a média
# MAGIC mensal da frota foi de US$ [Z].

