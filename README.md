# databricks-retail-lakehouse

Mini-Projekt für Databricks Community Edition mit Medallion Architecture (Bronze/Silver/Gold), Delta Lake, PySpark-first und SCD Type 2 für Kunden.

## Projektstruktur

```text
databricks-retail-lakehouse/
  data/
    customers.csv
    products.csv
    orders.csv
  notebooks/
    01_bronze_ingestion.py
    02_silver_transform.py
    03_gold_kpis.py
    04_scd2_customers.py
  sql/
    01_setup.sql
    02_bronze.sql
    03_silver.sql
    04_scd2.sql
    05_gold.sql
    06_validation.sql
  docs/
    data_dictionary.md
    architecture.png
    screenshots/
      .gitkeep
  README.md
```

## Setup in Databricks Community Edition

1. Repository in Databricks Repos importieren.
2. Lokalen `data/`-Ordner nach DBFS hochladen, z. B. nach:
   - `dbfs:/FileStore/databricks-retail-lakehouse/data`
3. Notebook-Reihenfolge ausführen:
   1. `notebooks/01_bronze_ingestion.py`
   2. `notebooks/02_silver_transform.py`
   3. `notebooks/04_scd2_customers.py`
   4. `notebooks/03_gold_kpis.py`

## Databricks SQL Runbook

Run-Reihenfolge im SQL Editor:

1. `sql/01_setup.sql`
2. `sql/02_bronze.sql`
3. `sql/03_silver.sql`
4. `sql/04_scd2.sql`
5. `sql/05_gold.sql`
6. `sql/06_validation.sql`

### Unity Catalog Volume Paths

- `/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/customers.csv`
- `/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/products.csv`
- `/Volumes/workspace/retail_lakehouse/retail_lakehouse_files/orders.csv`

### Erwartete Tabellen je Layer

Bronze:
- `retail_lakehouse.bronze_customers`
- `retail_lakehouse.bronze_products`
- `retail_lakehouse.bronze_orders`

Silver:
- `retail_lakehouse.silver_customers_current`
- `retail_lakehouse.silver_products`
- `retail_lakehouse.silver_order_lines`
- `retail_lakehouse.silver_customers_scd2`

Gold:
- `retail_lakehouse.gold_daily_revenue`
- `retail_lakehouse.gold_product_revenue`
- `retail_lakehouse.gold_top_customers`
- `retail_lakehouse.gold_aov`

### Validierungs-Queries

- `SHOW TABLES IN retail_lakehouse;`
- Row counts für:
  - `bronze_customers`
  - `silver_customers_current`
  - `silver_customers_scd2` (nur `is_current = true`)
  - `gold_daily_revenue`
- Dedupe Checks:
  - `silver_customers_current` darf keine mehrfachen `customer_id` enthalten
  - `silver_customers_scd2` darf für `is_current = true` keine mehrfachen `customer_id` enthalten

## Architektur (Bronze / Silver / Gold)

- Bronze:
  - Rohdaten aus CSV als Delta-Tabellen
  - Metadaten `ingestion_ts` und `source_file`
- Silver:
  - Typisierung, Null-Handling, Deduplizierung nach PK mit neuestem `updated_at`
  - Berechnung `line_revenue` durch Join von Orders und Products
- Gold:
  - KPI-Aggregationen für Reporting/BI

## SCD Type 2 (nur Customers)

Notebook `04_scd2_customers.py` pflegt `silver_customers_scd2` per Delta `MERGE`.

SQL-Variante (`sql/04_scd2.sql`) enthält einen klaren Initial-Load mit:
- Hash-basierter Versionslogik via `sha2(concat_ws('||', ...), 256)`
- `valid_from` = Ladezeitpunkt
- `valid_to` = `9999-12-31`
- `is_current` = aktueller Datensatzmarker

## KPI-Tabellen (Gold)

- `gold_daily_revenue`
- `gold_product_revenue`
- `gold_top_customers`
- `gold_aov`

## Beispielabfragen

```sql
USE retail_lakehouse;

SELECT * FROM gold_daily_revenue ORDER BY order_date;
SELECT * FROM gold_product_revenue ORDER BY total_revenue DESC LIMIT 10;
SELECT * FROM gold_top_customers ORDER BY total_revenue DESC LIMIT 10;
SELECT * FROM gold_aov;

SELECT * FROM silver_customers_scd2 WHERE is_current = true;
```

## Delta Lake Features im Projekt

- Delta-Tabellen mit `USING DELTA`
- Schema Enforcement
- ACID-Transaktionen
- Time Travel möglich, z. B.:

```sql
SELECT * FROM silver_customers_scd2 VERSION AS OF 0;
```

## Lessons Learned

- Medallion trennt Rohdaten, bereinigte Daten und KPI-Layer klar.
- Dedupe mit `ROW_NUMBER()` + `updated_at DESC` ist robust und nachvollziehbar.
- SCD2 über Hashing vereinfacht Change Detection.
- Delta Tables machen Pipelines wiederholbar und stabil.
- Eigene Validierungs-Queries verhindern stille Datenqualitätsfehler.
