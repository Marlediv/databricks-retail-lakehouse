-- 04_scd2.sql
-- SCD Type 2 für Kunden auf Basis von silver_customers_current.
CREATE DATABASE IF NOT EXISTS retail_lakehouse;
USE retail_lakehouse;

DROP TABLE IF EXISTS retail_lakehouse.silver_customers_scd2;

CREATE TABLE retail_lakehouse.silver_customers_scd2 (
  customer_id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  city STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  record_hash STRING
)
USING DELTA;

INSERT INTO retail_lakehouse.silver_customers_scd2
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  city,
  current_timestamp() AS valid_from,
  TIMESTAMP('9999-12-31 00:00:00') AS valid_to,
  true AS is_current,
  sha2(concat_ws('||', first_name, last_name, email, city), 256) AS record_hash
FROM retail_lakehouse.silver_customers_current;
