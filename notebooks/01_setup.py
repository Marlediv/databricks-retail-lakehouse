# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Setup
# MAGIC Erstellt und aktiviert das Zielschema `retail_lakehouse`.

# COMMAND ----------
from notebooks._spark import spark

DATABASE = "retail_lakehouse"

if spark is None:
    print("Skipping Spark work (no Spark runtime available).")
else:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")

    print(f"Setup complete. Active database: {DATABASE}")

    # COMMAND ----------
    # MAGIC %md
    # MAGIC ## Validation

    # COMMAND ----------
    spark.sql("SELECT current_database() AS active_database").show(truncate=False)
    spark.sql(f"SHOW TABLES IN {DATABASE}").show(truncate=False)
