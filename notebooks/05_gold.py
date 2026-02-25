# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Gold
# MAGIC Berechnet KPI-Tabellen für Tagesumsatz, Produktumsatz und Top-Kunden.

# COMMAND ----------
from pyspark.sql import functions as F
from notebooks._spark import spark

DATABASE = "retail_lakehouse"

if spark is None:
    print("Skipping Spark work (no Spark runtime available).")
else:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")

    silver_order_lines = spark.table(f"{DATABASE}.silver_order_lines")
    silver_customers_current = spark.table(f"{DATABASE}.silver_customers_current")

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
            F.sum("quantity").alias("total_qty"),
        )
    )

    # COMMAND ----------
    gold_top_customers = (
        silver_order_lines.alias("o")
        .join(silver_customers_current.alias("c"), on="customer_id", how="left")
        .groupBy("o.customer_id")
        .agg(F.sum("o.line_revenue").cast("decimal(14,2)").alias("total_revenue"))
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

    print(
        "Gold tables written: gold_daily_revenue, "
        "gold_product_revenue, gold_top_customers"
    )

    # COMMAND ----------
    # MAGIC %md
    # MAGIC ## Validation

    # COMMAND ----------
    for table_name in [
        "gold_daily_revenue",
        "gold_product_revenue",
        "gold_top_customers",
    ]:
        spark.sql(
            f"SELECT '{table_name}' AS table_name, COUNT(*) AS row_count "
            f"FROM {DATABASE}.{table_name}"
        ).show(truncate=False)
