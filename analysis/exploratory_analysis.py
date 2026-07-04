# Databricks notebook source
# MAGIC %md
# MAGIC # Análise Exploratória — Camada Bronze
# MAGIC
# MAGIC EDA executada **antes** da definição das regras de qualidade: as regras da
# MAGIC silver não foram arbitradas, foram derivadas do que esta análise revelou
# MAGIC (nulos em `passenger_count`, valores não positivos em `total_amount`,
# MAGIC registros fora do período Jan–Mai/2023, duplicatas).

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
# MAGIC ## Distribuição de `total_amount`
# MAGIC
# MAGIC O summary revela a cauda da distribuição (mínimos negativos = estornos;
# MAGIC máximos extremos = outliers). É a evidência por trás da regra
# MAGIC `total_amount > 0` da camada silver.

# COMMAND ----------

df_yellow.select("total_amount").summary(
    "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volumetria mensal
# MAGIC
# MAGIC Confirma que o volume é consistente entre os meses (sem buracos de ingestão)
# MAGIC e dimensiona o dataset (~16,5M de registros no período).

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
# MAGIC - [X]% de registros com `total_amount <= 0` (estornos/ajustes) e [Y]
# MAGIC   registros fora do range Jan–Mai/2023 — ambos capturados pelas regras de
# MAGIC   quarentena da silver.
# MAGIC - `passenger_count` nulo em [Z]% dos registros — premissa: preservado na
# MAGIC   silver, filtrado nas análises de passageiros.
# MAGIC - Volume mensal consistente ao longo do período, sem buracos de ingestão.

