# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
# Configuration
DATABASE = "retail_lakehouse"
CUSTOMERS_PATH = "/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/customers.csv"
PRODUCTS_PATH = "/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/products.csv"
ORDERS_PATH = "/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/orders.csv"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

# COMMAND ----------
def load_bronze_csv(path: str):
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)
        .load(path)
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )

# COMMAND ----------
bronze_customers = load_bronze_csv(CUSTOMERS_PATH)
bronze_products = load_bronze_csv(PRODUCTS_PATH)
bronze_orders = load_bronze_csv(ORDERS_PATH)

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
# Validation
validation_tables = [
    "bronze_customers",
    "bronze_products",
    "bronze_orders",
]

for table_name in validation_tables:
    full_name = f"{DATABASE}.{table_name}"
    count_df = spark.sql(f"SELECT '{table_name}' AS table_name, COUNT(*) AS row_count FROM {full_name}")
    count_df.show(truncate=False)

if spark.catalog.tableExists(f"{DATABASE}.silver_customers_current"):
    print("Dedupe check: silver_customers_current")
    spark.sql(
        f"""
        SELECT customer_id, COUNT(*) AS cnt
        FROM {DATABASE}.silver_customers_current
        GROUP BY customer_id
        HAVING COUNT(*) > 1
        """
    ).show(truncate=False)
else:
    print("Dedupe check skipped: silver_customers_current does not exist yet.")

if spark.catalog.tableExists(f"{DATABASE}.silver_customers_scd2"):
    print("SCD2 check: multiple current rows")
    spark.sql(
        f"""
        SELECT customer_id, COUNT(*) AS current_rows
        FROM {DATABASE}.silver_customers_scd2
        WHERE is_current = true
        GROUP BY customer_id
        HAVING COUNT(*) > 1
        """
    ).show(truncate=False)
else:
    print("SCD2 check skipped: silver_customers_scd2 does not exist yet.")
