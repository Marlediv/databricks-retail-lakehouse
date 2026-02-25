# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver
# MAGIC Typisierung, Deduplizierung und Anreicherung fuer Kunden, Produkte und
# MAGIC Order-Lines.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from notebooks._spark import spark

DATABASE = "retail_lakehouse"

if spark is None:
    print("Skipping Spark work (no Spark runtime available).")
else:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")

    # COMMAND ----------
    bronze_customers = spark.table(f"{DATABASE}.bronze_customers")
    bronze_products = spark.table(f"{DATABASE}.bronze_products")
    bronze_orders = spark.table(f"{DATABASE}.bronze_orders")

    # COMMAND ----------
    def dedupe_latest(df, key_col: str):
        w = Window.partitionBy(key_col).orderBy(
            F.col("updated_at").desc(), F.col("ingestion_ts").desc()
        )
        return (
            df.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

    # COMMAND ----------
    silver_products = bronze_products.select(
        F.col("product_id").cast("int").alias("product_id"),
        F.col("product_name").cast("string").alias("product_name"),
        F.col("category").cast("string").alias("category"),
        F.col("price").cast("decimal(10,2)").alias("price"),
        F.to_timestamp("updated_at").alias("updated_at"),
        F.col("ingestion_ts").cast("timestamp").alias("ingestion_ts"),
        F.col("source_file").cast("string").alias("source_file"),
    )

    # COMMAND ----------
    customers_typed = bronze_customers.select(
        F.col("customer_id").cast("int").alias("customer_id"),
        F.coalesce(F.col("first_name").cast("string"), F.lit("Unknown")).alias(
            "first_name"
        ),
        F.coalesce(F.col("last_name").cast("string"), F.lit("Unknown")).alias(
            "last_name"
        ),
        F.coalesce(
            F.col("email").cast("string"),
            F.concat(
                F.lit("unknown_"),
                F.col("customer_id").cast("string"),
                F.lit("@example.com"),
            ),
        ).alias("email"),
        F.coalesce(F.col("city").cast("string"), F.lit("Unknown")).alias("city"),
        F.to_date("signup_date").alias("signup_date"),
        F.to_timestamp("updated_at").alias("updated_at"),
        F.col("ingestion_ts").cast("timestamp").alias("ingestion_ts"),
        F.col("source_file").cast("string").alias("source_file"),
    ).filter(F.col("customer_id").isNotNull())

    silver_customers_current = dedupe_latest(customers_typed, "customer_id")

    # COMMAND ----------
    orders_typed = bronze_orders.select(
        F.col("order_line_id").cast("int").alias("order_line_id"),
        F.col("order_id").cast("int").alias("order_id"),
        F.to_date("order_date").alias("order_date"),
        F.col("customer_id").cast("int").alias("customer_id"),
        F.col("product_id").cast("int").alias("product_id"),
        F.coalesce(F.col("quantity").cast("int"), F.lit(1)).alias("quantity"),
        F.to_timestamp("updated_at").alias("updated_at"),
        F.col("ingestion_ts").cast("timestamp").alias("ingestion_ts"),
        F.col("source_file").cast("string").alias("source_file"),
    ).filter(F.col("order_line_id").isNotNull())

    dedup_orders = dedupe_latest(orders_typed, "order_line_id")

    silver_order_lines = (
        dedup_orders.alias("o")
        .join(silver_products.alias("p"), on="product_id", how="left")
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
            (F.col("o.quantity") * F.col("p.price"))
            .cast("decimal(12,2)")
            .alias("line_revenue"),
            F.col("o.updated_at"),
            F.col("o.ingestion_ts"),
            F.col("o.source_file"),
        )
    )

    # COMMAND ----------
    (
        silver_customers_current.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{DATABASE}.silver_customers_current")
    )
    (
        silver_products.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{DATABASE}.silver_products")
    )
    (
        silver_order_lines.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{DATABASE}.silver_order_lines")
    )

    print(
        "Silver tables written: silver_customers_current, "
        "silver_products, silver_order_lines"
    )

    # COMMAND ----------
    # MAGIC %md
    # MAGIC ## Validation

    # COMMAND ----------
    for table_name in [
        "silver_customers_current",
        "silver_products",
        "silver_order_lines",
    ]:
        spark.sql(
            f"SELECT '{table_name}' AS table_name, COUNT(*) AS row_count "
            f"FROM {DATABASE}.{table_name}"
        ).show(truncate=False)

    spark.sql(f"""
        SELECT customer_id, COUNT(*) AS row_count
        FROM {DATABASE}.silver_customers_current
        GROUP BY customer_id
        HAVING COUNT(*) > 1
        """).show(truncate=False)
