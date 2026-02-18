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
  docs/
    data_dictionary.md
    architecture.png
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
4. Optional im Bronze-Notebook Widget `source_base_path` anpassen, falls ein anderer Upload-Pfad verwendet wird.

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

Notebook `04_scd2_customers.py` pflegt `silver_customers_scd2` per Delta `MERGE`:

- Wenn aktueller Datensatz (`is_current=true`) sich in den fachlichen Attributen ändert (`record_hash`):
  - alten Datensatz schließen (`valid_to = current_timestamp`, `is_current = false`)
  - neuen aktuellen Datensatz einfügen
- Wenn Kunde neu ist:
  - direkt als aktueller Datensatz einfügen

Die Logik ist idempotent: erneute Ausführung ohne neue Änderungen erzeugt keine Duplikate.

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

-- SCD2 aktuelle Kundensicht
SELECT * FROM silver_customers_scd2 WHERE is_current = true;
```

## Delta Lake Features im Projekt

- Schema Enforcement über Delta-Tabellen
- ACID-Transaktionen für konsistente Schreibvorgänge
- Time Travel möglich, z. B.:

```sql
SELECT * FROM silver_customers_scd2 VERSION AS OF 0;
```

## Hinweise zur Idempotenz

- Bronze/Silver/Gold Writes mit `mode("overwrite")`
- SCD2 mit deterministischer `MERGE`-Logik (Expire + Insert)
- Notebooks können mehrfach ausgeführt werden, ohne unkontrollierte Duplikate
