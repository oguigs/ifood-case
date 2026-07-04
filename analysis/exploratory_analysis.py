# Databricks notebook source
# MAGIC %md
# MAGIC # Análise Exploratória — Camada Bronze
# MAGIC
# MAGIC Rodei essa EDA antes de decidir qualquer regra de qualidade — não o
# MAGIC contrário. As regras que acabaram na camada silver (nulos em
# MAGIC `passenger_count`, valores não positivos em `total_amount`, registros fora
# MAGIC do período Jan–Mai/2023, duplicatas) saíram do que essa análise mostrou,
# MAGIC não de suposição.

# COMMAND ----------

from pyspark.sql import functions as F

df_yellow = spark.table("ifood_case.bronze.yellow_taxi_trips")

df_yellow.select(
    F.count("*").alias("total"),
    F.sum(F.col("passenger_count").isNull().cast("int")).alias("passenger_null"),
    F.sum((F.col("total_amount") <= 0).cast("int")).alias("amount_nao_positivo"),
    F.sum((F.col("tpep_pickup_datetime") < "2023-01-01").cast("int")).alias("antes_jan"),
    F.sum((F.col("tpep_pickup_datetime") >= "2023-06-01").cast("int")).alias("depois_mai"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribuição de total_amount
# MAGIC
# MAGIC Dando uma olhada no summary dá pra ver a cauda da distribuição — mínimos
# MAGIC negativos (que são estornos) e máximos bem fora da curva (outliers). Foi
# MAGIC essa distribuição que me convenceu a colocar a regra `total_amount > 0` na
# MAGIC silver.

# COMMAND ----------

df_yellow.select("total_amount").summary(
    "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volumetria mensal
# MAGIC
# MAGIC Serve pra checar duas coisas ao mesmo tempo: se o volume é estável entre os
# MAGIC meses (sem buraco de ingestão) e pra ter uma noção de tamanho do dataset
# MAGIC estamos falando de algo em torno de 16,5 milhões de registros no período.

# COMMAND ----------

(
    df_yellow.withColumn("mes", F.date_format("tpep_pickup_datetime", "yyyy-MM"))
    .groupBy("mes")
    .count()
    .orderBy("mes")
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusões da EDA
# MAGIC
# MAGIC - **2,65%** dos registros (428.665 de 16.186.386) vieram com
# MAGIC   `passenger_count` nulo. Decidi manter esses registros na silver — a
# MAGIC   ausência de preenchimento pelo taxímetro não invalida a corrida pra quem
# MAGIC   quer analisar receita — e só filtrar o nulo nas análises que dependem de
# MAGIC   passageiros, como a pergunta 2.
# MAGIC - **0,89%** (144.146 registros) têm `total_amount <= 0`. A distribuição
# MAGIC   ajuda a entender por quê: o mínimo chega a -US$ 982,95 (claramente um
# MAGIC   estorno), bem longe da mediana de US$ 20,60. Essa é a regra
# MAGIC   `non_positive_total_amount` que vira quarentena na silver.
# MAGIC - Olhando só pelo recorte estrito de datas, apenas 104 registros (67 antes
# MAGIC   de janeiro, 37 depois de maio) ficam fora do range pedido. Mas a
# MAGIC   volumetria mensal conta uma história mais interessante: tem registro
# MAGIC   espalhado em anos completamente fora de qualquer contexto — 2001, 2002,
# MAGIC   2003, 2008, 2009, 2014, 2022. Isso é claramente erro de relógio na fonte
# MAGIC   da TLC, e é um bom exemplo de como uma olhada rápida nas bordas do
# MAGIC   período não conta a história inteira — só apareceu de verdade ao agrupar
# MAGIC   por mês.
# MAGIC - Fora isso, o volume mensal se mantém estável entre janeiro e maio (na
# MAGIC   faixa de 3 a 3,5 milhões de registros por mês), sem sinal de falha de
# MAGIC   ingestão em nenhum mês.
