# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
# Optional: in Databricks CE, set this widget to your uploaded CSV location.
# Example: dbfs:/FileStore/databricks-retail-lakehouse/data
try:
    dbutils.widgets.text("source_base_path", "dbfs:/FileStore/databricks-retail-lakehouse/data")
    source_base_path = dbutils.widgets.get("source_base_path")
except NameError:
    source_base_path = "data"

DATABASE = "retail_lakehouse"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

# COMMAND ----------
def load_csv_with_metadata(file_name: str):
    path = f"{source_base_path}/{file_name}"
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)
        .load(path)
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )

# COMMAND ----------
bronze_customers = load_csv_with_metadata("customers.csv")
bronze_products = load_csv_with_metadata("products.csv")
bronze_orders = load_csv_with_metadata("orders.csv")

# COMMAND ----------
(bronze_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_customers"))
(bronze_products.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_products"))
(bronze_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_orders"))

print("Bronze tables written: bronze_customers, bronze_products, bronze_orders")
