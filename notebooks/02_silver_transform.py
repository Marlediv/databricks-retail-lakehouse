# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
DATABASE = "retail_lakehouse"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

def deduplicate_latest(df, pk_col: str):
    w = Window.partitionBy(pk_col).orderBy(F.col("updated_at").desc(), F.col("ingestion_ts").desc())
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

# COMMAND ----------
bronze_customers = spark.table("bronze_customers")
bronze_products = spark.table("bronze_products")
bronze_orders = spark.table("bronze_orders")

# COMMAND ----------
customers_typed = (
    bronze_customers.select(
        F.col("customer_id").cast("int").alias("customer_id"),
        F.coalesce(F.col("first_name"), F.lit("Unknown")).alias("first_name"),
        F.coalesce(F.col("last_name"), F.lit("Unknown")).alias("last_name"),
        F.coalesce(F.col("email"), F.concat(F.lit("unknown_"), F.col("customer_id"), F.lit("@example.com"))).alias("email"),
        F.coalesce(F.col("city"), F.lit("Unknown")).alias("city"),
        F.to_date("signup_date").alias("signup_date"),
        F.to_timestamp("updated_at").alias("updated_at"),
        F.col("ingestion_ts"),
        F.col("source_file"),
    )
    .filter(F.col("customer_id").isNotNull())
)
customers_silver = deduplicate_latest(customers_typed, "customer_id")

# COMMAND ----------
products_typed = (
    bronze_products.select(
        F.col("product_id").cast("int").alias("product_id"),
        F.coalesce(F.col("product_name"), F.lit("Unknown Product")).alias("product_name"),
        F.coalesce(F.col("category"), F.lit("Uncategorized")).alias("category"),
        F.coalesce(F.col("price").cast("double"), F.lit(0.0)).alias("price"),
        F.to_timestamp("updated_at").alias("updated_at"),
        F.col("ingestion_ts"),
        F.col("source_file"),
    )
    .filter(F.col("product_id").isNotNull())
)
products_silver = deduplicate_latest(products_typed, "product_id")

# COMMAND ----------
orders_typed = (
    bronze_orders.select(
        F.col("order_line_id").cast("int").alias("order_line_id"),
        F.col("order_id").cast("int").alias("order_id"),
        F.to_date("order_date").alias("order_date"),
        F.col("customer_id").cast("int").alias("customer_id"),
        F.col("product_id").cast("int").alias("product_id"),
        F.coalesce(F.col("quantity").cast("int"), F.lit(1)).alias("quantity"),
        F.to_timestamp("updated_at").alias("updated_at"),
        F.col("ingestion_ts"),
        F.col("source_file"),
    )
    .filter(F.col("order_line_id").isNotNull())
)
orders_silver = deduplicate_latest(orders_typed, "order_line_id")

# COMMAND ----------
silver_order_lines = (
    orders_silver.alias("o")
    .join(products_silver.alias("p"), on="product_id", how="left")
    .select(
        F.col("o.order_line_id"),
        F.col("o.order_id"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
        F.col("o.product_id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("o.quantity"),
        (F.col("o.quantity") * F.coalesce(F.col("p.price"), F.lit(0.0))).alias("line_revenue"),
        F.col("o.updated_at"),
    )
)

# COMMAND ----------
(customers_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_customers_current"))
(products_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_products"))
(silver_order_lines.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_order_lines"))

print("Silver tables written: silver_customers_current, silver_products, silver_order_lines")
