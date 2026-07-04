# Databricks notebook source
# MAGIC %md
# MAGIC # Pergunta 2 — Média de passageiros por hora do dia em maio (todos os táxis)
# MAGIC
# MAGIC "Todos os táxis da frota" é a parte que mais importa nessa pergunta — por
# MAGIC isso a query roda sobre yellow e green juntos, já unificados na silver pela
# MAGIC coluna `taxi_type`. O filtro de maio usa as colunas de partição, então o
# MAGIC Spark nem chega a olhar os outros meses.
# MAGIC
# MAGIC Um detalhe que decidi na hora de escrever essa query: registros com
# MAGIC `passenger_count` nulo entram fora do cálculo aqui, mas só aqui — eles
# MAGIC continuam existindo na silver normalmente, porque campo vazio não é a
# MAGIC mesma coisa que zero passageiro, e não queria distorcer a média por causa
# MAGIC disso.

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
# MAGIC O padrão que aparece no gráfico faz bastante sentido intuitivo: a média
# MAGIC sobe na madrugada (0h às 3h, por volta de 1,45 passageiros) e de novo à
# MAGIC noite (20h às 23h) — provavelmente gente voltando em grupo de algum
# MAGIC compromisso social. O ponto mais baixo é às 6h da manhã, perto de 1,26, que
# MAGIC bate com o horário de quem está indo sozinho para o trabalho. No resto do
# MAGIC horário comercial (7h às 19h) a média fica bem estável, entre 1,35 e 1,40.
