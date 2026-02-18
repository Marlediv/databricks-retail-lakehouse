# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
DATABASE = "retail_lakehouse"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

order_lines = spark.table("silver_order_lines")
customers = spark.table("silver_customers_current")

# COMMAND ----------
gold_daily_revenue = (
    order_lines.groupBy("order_date")
    .agg(
        F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_items"),
    )
    .orderBy("order_date")
)

# COMMAND ----------
gold_product_revenue = (
    order_lines.groupBy("product_id", "product_name", "category")
    .agg(
        F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy(F.desc("total_revenue"))
)

# COMMAND ----------
gold_top_customers = (
    order_lines.alias("o")
    .join(customers.alias("c"), on="customer_id", how="left")
    .groupBy("o.customer_id", "c.first_name", "c.last_name", "c.email", "c.city")
    .agg(
        F.round(F.sum("o.line_revenue"), 2).alias("total_revenue"),
        F.countDistinct("o.order_id").alias("order_count"),
    )
    .orderBy(F.desc("total_revenue"))
)

# COMMAND ----------
gold_aov = (
    order_lines.agg(
        F.round(F.sum("line_revenue"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("order_count"),
    )
    .withColumn(
        "aov",
        F.when(F.col("order_count") > 0, F.round(F.col("total_revenue") / F.col("order_count"), 2)).otherwise(F.lit(0.0)),
    )
    .withColumn("calculated_at", F.current_timestamp())
)

# COMMAND ----------
(gold_daily_revenue.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_daily_revenue"))
(gold_product_revenue.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_product_revenue"))
(gold_top_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_top_customers"))
(gold_aov.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_aov"))

print("Gold tables written: gold_daily_revenue, gold_product_revenue, gold_top_customers, gold_aov")
