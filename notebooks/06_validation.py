# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Validation
# MAGIC Prüft Tabellenbestand, Counts, Dedupe-Regeln und zeigt Gold-Previews.

# COMMAND ----------
from notebooks._spark import spark


DATABASE = "retail_lakehouse"

expected_tables = [
    "bronze_customers",
    "bronze_products",
    "bronze_orders",
    "silver_customers_current",
    "silver_products",
    "silver_order_lines",
    "silver_customers_scd2",
    "gold_daily_revenue",
    "gold_product_revenue",
    "gold_top_customers",
]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------
if spark is None:
    print("Skipping Spark checks (no Spark runtime available).")
else:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")

    print("Tables in schema:")
    spark.sql(f"SHOW TABLES IN {DATABASE}").show(truncate=False)

    # COMMAND ----------
    print("Row counts:")
    for table_name in expected_tables:
        spark.sql(
            f"SELECT '{table_name}' AS table_name, COUNT(*) AS row_count "
            f"FROM {DATABASE}.{table_name}"
        ).show(truncate=False)

    # COMMAND ----------
    print("Dedupe check: silver_customers_current")
    spark.sql(
        f"""
        SELECT customer_id, COUNT(*) AS row_count
        FROM {DATABASE}.silver_customers_current
        GROUP BY customer_id
        HAVING COUNT(*) > 1
        """
    ).show(truncate=False)

    print("SCD2 current check: silver_customers_scd2")
    spark.sql(
        f"""
        SELECT customer_id, COUNT(*) AS current_rows
        FROM {DATABASE}.silver_customers_scd2
        WHERE is_current = true
        GROUP BY customer_id
        HAVING COUNT(*) > 1
        """
    ).show(truncate=False)

    # COMMAND ----------
    print("Gold preview: gold_daily_revenue")
    spark.sql(
        f"SELECT * FROM {DATABASE}.gold_daily_revenue ORDER BY order_date DESC LIMIT 10"
    ).show(truncate=False)

    print("Gold preview: gold_product_revenue")
    spark.sql(
        f"SELECT * FROM {DATABASE}.gold_product_revenue "
        "ORDER BY total_revenue DESC LIMIT 10"
    ).show(truncate=False)

    print("Gold preview: gold_top_customers")
    spark.sql(
        f"SELECT * FROM {DATABASE}.gold_top_customers "
        "ORDER BY total_revenue DESC LIMIT 10"
    ).show(truncate=False)
