# Databricks notebook source
# MAGIC %md
# MAGIC # 04 SCD2
# MAGIC Baut/aktualisiert die SCD2-Kundentabelle via MERGE mit Hash-basierter Änderungslogik.

# COMMAND ----------
from pyspark.sql import functions as F

DATABASE = "retail_lakehouse"
TARGET_TABLE = f"{DATABASE}.silver_customers_scd2"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

# COMMAND ----------
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
      customer_id INT,
      first_name STRING,
      last_name STRING,
      email STRING,
      city STRING,
      valid_from TIMESTAMP,
      valid_to TIMESTAMP,
      is_current BOOLEAN,
      record_hash STRING
    ) USING DELTA
    """
)

# COMMAND ----------
scd2_source = (
    spark.table(f"{DATABASE}.silver_customers_current")
    .select("customer_id", "first_name", "last_name", "email", "city")
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "|",
                F.coalesce(F.col("first_name"), F.lit("")),
                F.coalesce(F.col("last_name"), F.lit("")),
                F.coalesce(F.col("email"), F.lit("")),
                F.coalesce(F.col("city"), F.lit("")),
            ),
            256,
        ),
    )
)

scd2_source.createOrReplaceTempView("stg_scd2_customers")

# COMMAND ----------
# 1) Close current records when tracked attributes changed.
spark.sql(
    f"""
    MERGE INTO {TARGET_TABLE} AS tgt
    USING stg_scd2_customers AS src
    ON tgt.customer_id = src.customer_id AND tgt.is_current = true
    WHEN MATCHED AND tgt.record_hash <> src.record_hash THEN
      UPDATE SET
        tgt.valid_to = current_timestamp(),
        tgt.is_current = false
    """
)

# COMMAND ----------
# 2) Insert new customers and new current versions for changed customers.
spark.sql(
    f"""
    MERGE INTO {TARGET_TABLE} AS tgt
    USING stg_scd2_customers AS src
    ON tgt.customer_id = src.customer_id AND tgt.is_current = true
    WHEN NOT MATCHED THEN
      INSERT (
        customer_id,
        first_name,
        last_name,
        email,
        city,
        valid_from,
        valid_to,
        is_current,
        record_hash
      )
      VALUES (
        src.customer_id,
        src.first_name,
        src.last_name,
        src.email,
        src.city,
        current_timestamp(),
        TIMESTAMP('9999-12-31 00:00:00'),
        true,
        src.record_hash
      )
    """
)

print("SCD2 merge finished: silver_customers_scd2")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------
spark.sql(
    f"SELECT COUNT(*) AS row_count FROM {TARGET_TABLE}"
).show(truncate=False)

duplicate_current = spark.sql(
    f"""
    SELECT customer_id, COUNT(*) AS current_rows
    FROM {TARGET_TABLE}
    WHERE is_current = true
    GROUP BY customer_id
    HAVING COUNT(*) > 1
    """
)

duplicate_current.show(truncate=False)

if duplicate_current.limit(1).count() > 0:
    raise ValueError("SCD2 validation failed: more than one current row per customer_id found.")
