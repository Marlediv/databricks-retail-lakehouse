# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
# Configuration
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
source_current = (
    spark.table(f"{DATABASE}.silver_customers_current")
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
# Expire changed current records.
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
# Insert new customers and changed versions.
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
        TIMESTAMP('9999-12-31 00:00:00'),
        true,
        src.record_hash
      )
    """
)

print("SCD2 table merged: silver_customers_scd2")

# COMMAND ----------
# Validation
spark.sql(
    f"SELECT 'silver_customers_scd2' AS table_name, COUNT(*) AS row_count FROM {TARGET_TABLE}"
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

scd2_dupes = spark.sql(
    f"""
    SELECT customer_id, COUNT(*) AS current_rows
    FROM {TARGET_TABLE}
    WHERE is_current = true
    GROUP BY customer_id
    HAVING COUNT(*) > 1
    """
)

print("SCD2 check: multiple current rows")
scd2_dupes.show(truncate=False)

if scd2_dupes.limit(1).count() > 0:
    raise ValueError("SCD2 validation failed: multiple current rows found for at least one customer_id.")
