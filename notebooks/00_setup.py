# Databricks notebook source
# MAGIC %md
# MAGIC # Setup — Unity Catalog
# MAGIC
# MAGIC Criação do catálogo, schemas e volume que sustentam a arquitetura medallion.
# MAGIC
# MAGIC **Por que Unity Catalog:** resolve a exigência de "tecnologia de metadados"
# MAGIC do enunciado com governança nativa — namespace de 3 níveis
# MAGIC (`catalogo.schema.tabela`), lineage automático entre camadas e catálogo
# MAGIC self-service, alinhado à "Fonte Única da Verdade" descrita na vaga.
# MAGIC
# MAGIC **Por que um Volume como landing zone:** equivale ao bucket S3 sugerido no
# MAGIC enunciado, mas com storage gerenciado pelo próprio catálogo — zero
# MAGIC credenciais externas, o que torna a solução 100% reproduzível pelo avaliador
# MAGIC no Databricks Free Edition.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ifood_case;
# MAGIC CREATE SCHEMA  IF NOT EXISTS ifood_case.bronze;
# MAGIC CREATE SCHEMA  IF NOT EXISTS ifood_case.silver;
# MAGIC CREATE SCHEMA  IF NOT EXISTS ifood_case.gold;
# MAGIC CREATE VOLUME  IF NOT EXISTS ifood_case.bronze.landing;

