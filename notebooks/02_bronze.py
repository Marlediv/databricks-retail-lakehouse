# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Bronze
# MAGIC Lädt CSV-Dateien aus Unity Catalog Volumes und schreibt Bronze-Delta-Tabellen.

# COMMAND ----------
from pyspark.sql import functions as F

DATABASE = "retail_lakehouse"

CUSTOMERS_PATH = "/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/customers.csv"
PRODUCTS_PATH = "/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/products.csv"
ORDERS_PATH = "/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/orders.csv"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

# COMMAND ----------
def load_csv_with_metadata(path: str):
    return (
        spark.read.format("csv")
        .option("header", True)
        .load(path)
        .withColumn("ingestion_ts", F.current_timestamp())
        # Unity Catalog: input_file_name() nicht unterstützt, nutze _metadata.file_path
        .withColumn("source_file", F.col("_metadata.file_path"))
    )

# COMMAND ----------
bronze_customers = load_csv_with_metadata(CUSTOMERS_PATH)
bronze_products = load_csv_with_metadata(PRODUCTS_PATH)
bronze_orders = load_csv_with_metadata(ORDERS_PATH)

# COMMAND ----------
(
    bronze_customers.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DATABASE}.bronze_customers")
)
(
    bronze_products.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DATABASE}.bronze_products")
)
(
    bronze_orders.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DATABASE}.bronze_orders")
)

print("Bronze tables written: bronze_customers, bronze_products, bronze_orders")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------
for table_name in ["bronze_customers", "bronze_products", "bronze_orders"]:
    spark.sql(
        f"SELECT '{table_name}' AS table_name, COUNT(*) AS row_count FROM {DATABASE}.{table_name}"
    ).show(truncate=False)
