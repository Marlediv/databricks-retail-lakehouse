# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
# Configuration
DATABASE = "retail_lakehouse"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

silver_order_lines = spark.table(f"{DATABASE}.silver_order_lines")

# COMMAND ----------
gold_daily_revenue = (
    silver_order_lines.groupBy("order_date")
    .agg(
        F.sum("line_revenue").cast("decimal(14,2)").alias("daily_revenue"),
        F.countDistinct("order_id").alias("orders"),
    )
)

# COMMAND ----------
gold_product_revenue = (
    silver_order_lines.groupBy("product_id")
    .agg(
        F.sum("line_revenue").cast("decimal(14,2)").alias("total_revenue"),
        F.sum("quantity").alias("total_quantity"),
    )
)

# COMMAND ----------
gold_top_customers = (
    silver_order_lines.groupBy("customer_id")
    .agg(
        F.sum("line_revenue").cast("decimal(14,2)").alias("total_revenue"),
    )
)

# COMMAND ----------
(
    gold_daily_revenue.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DATABASE}.gold_daily_revenue")
)
(
    gold_product_revenue.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DATABASE}.gold_product_revenue")
)
(
    gold_top_customers.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DATABASE}.gold_top_customers")
)

print("Gold tables written: gold_daily_revenue, gold_product_revenue, gold_top_customers")

# COMMAND ----------
# Validation
for table_name in ["gold_daily_revenue", "gold_product_revenue", "gold_top_customers"]:
    spark.sql(
        f"SELECT '{table_name}' AS table_name, COUNT(*) AS row_count FROM {DATABASE}.{table_name}"
    ).show(truncate=False)

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
