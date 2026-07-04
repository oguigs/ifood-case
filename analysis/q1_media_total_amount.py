# Databricks notebook source
# MAGIC %md
# MAGIC # Pergunta 1 — Média de total_amount recebido em um mês (yellow taxis)
# MAGIC
# MAGIC O jeito como essa pergunta foi escrita dá margem para duas respostas
# MAGIC diferentes, então calculei as duas em vez de escolher uma sozinho:
# MAGIC
# MAGIC 1. Média por corrida, mês a mês (a leitura que considero principal): quanto
# MAGIC    rendeu, em média, uma corrida em cada mês.
# MAGIC 2. Receita média mensal da frota: soma tudo que entrou no mês e tira a
# MAGIC    média entre os 5 meses.
# MAGIC
# MAGIC A diferença entre as duas não é sutil — uma responde "qual o ticket médio"
# MAGIC e a outra "qual o faturamento médio mensal". Preferi deixar as duas
# MAGIC explícitas a arriscar responder a pergunta errada.
# MAGIC
# MAGIC A query usa as colunas de partição (`pickup_year`, `pickup_month`), então o
# MAGIC Spark já poda as partições que não interessam antes de ler qualquer dado.

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
# MAGIC | Mês | Média total_amount | Qtd. corridas |
# MAGIC |---|---|---|
# MAGIC | Jan/2023 | US$ 27,50 | 2.988.829 |
# MAGIC | Fev/2023 | US$ 27,39 | 2.840.168 |
# MAGIC | Mar/2023 | US$ 28,32 | 3.313.632 |
# MAGIC | Abr/2023 | US$ 28,82 | 3.199.973 |
# MAGIC | Mai/2023 | US$ 29,53 | 3.420.646 |
# MAGIC
# MAGIC A média por corrida foi de US$ 27,39 em fevereiro (o mês mais baixo) até
# MAGIC US$ 29,53 em maio (o mais alto). A tendência é de alta praticamente o tempo
# MAGIC todo — só fevereiro quebra a sequência com uma leve queda em relação a
# MAGIC janeiro, o que faz sentido já que é o mês mais curto do ano. De março pra
# MAGIC maio o crescimento é bem consistente, dando uma alta acumulada de
# MAGIC aproximadamente 4,3%.
# MAGIC
# MAGIC Já pela leitura alternativa, olhando a receita total agregada por mês, a
# MAGIC média mensal da frota fica em US$ 89.419.184,41.
