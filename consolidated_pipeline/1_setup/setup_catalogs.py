# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF not EXISTS fmcg;
# MAGIC USE CATALOG fmcg;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists fmcg.gold;
# MAGIC create schema if not exists fmcg.silver;
# MAGIC create schema if not exists fmcg.bronze;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from fmcg.gold.fact_orders;

# COMMAND ----------

