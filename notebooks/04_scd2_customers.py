# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
DATABASE = "retail_lakehouse"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

TARGET_TABLE = "silver_customers_scd2"

# COMMAND ----------
# Ensure target table exists with expected schema.
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
source_current = (
    spark.table("silver_customers_current")
    .select("customer_id", "first_name", "last_name", "email", "city")
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("first_name"), F.lit("")),
                F.coalesce(F.col("last_name"), F.lit("")),
                F.coalesce(F.col("email"), F.lit("")),
                F.coalesce(F.col("city"), F.lit("")),
            ),
            256,
        ),
    )
)
source_current.createOrReplaceTempView("stg_customers_current")

# COMMAND ----------
# 1) Expire changed current records.
spark.sql(
    f"""
    MERGE INTO {TARGET_TABLE} AS tgt
    USING stg_customers_current AS src
    ON tgt.customer_id = src.customer_id AND tgt.is_current = true
    WHEN MATCHED AND tgt.record_hash <> src.record_hash THEN
      UPDATE SET
        tgt.valid_to = current_timestamp(),
        tgt.is_current = false
    """
)

# COMMAND ----------
# 2) Insert new customers and newly changed versions.
spark.sql(
    f"""
    MERGE INTO {TARGET_TABLE} AS tgt
    USING stg_customers_current AS src
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
        CAST(NULL AS TIMESTAMP),
        true,
        src.record_hash
      )
    """
)

print("SCD2 table updated: silver_customers_scd2")
