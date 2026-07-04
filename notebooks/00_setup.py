# Databricks notebook source
# MAGIC %md
# MAGIC # Setup — Unity Catalog
# MAGIC
# MAGIC Primeiro passo: criar o catálogo, os schemas e o volume que vão sustentar
# MAGIC toda a arquitetura medallion do case.
# MAGIC
# MAGIC Optei pelo Unity Catalog porque ele já resolve, de graça, a exigência do
# MAGIC enunciado de escolher "uma tecnologia de metadados". Com o namespace de 3
# MAGIC níveis (`catalogo.schema.tabela`) eu ganho lineage automático entre as
# MAGIC camadas e um catálogo navegável — o que conversa direto com a ideia de
# MAGIC "Fonte Única da Verdade" que aparece na descrição da vaga.
# MAGIC
# MAGIC O volume faz o papel da landing zone (o enunciado sugere um bucket S3, mas
# MAGIC deixa livre a escolha da tecnologia). Usando um Volume gerenciado pelo
# MAGIC próprio catálogo eu não preciso de nenhuma credencial de nuvem externa, e
# MAGIC isso deixa a solução reproduzível por qualquer pessoa direto no Databricks
# MAGIC Free Edition, sem fricção de setup.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ifood_case;
# MAGIC CREATE SCHEMA  IF NOT EXISTS ifood_case.bronze;
# MAGIC CREATE SCHEMA  IF NOT EXISTS ifood_case.silver;
# MAGIC CREATE SCHEMA  IF NOT EXISTS ifood_case.gold;
# MAGIC CREATE VOLUME  IF NOT EXISTS ifood_case.bronze.landing;
