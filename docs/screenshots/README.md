# Screenshot-Dokumentation

Dieses Verzeichnis enthält visuelle Nachweise der implementierten Lakehouse-Architektur in Databricks.

Die Screenshots dienen als Beleg für:
- korrekte Tabellenanlage je Layer
- Datenvalidierung
- funktionierende KPI-Berechnungen
- SCD2-Implementierung

---

## Layer-Validierung

### 01_bronze_preview.png
SQL Editor Ansicht der Bronze-Tabellen im Schema `retail_lakehouse`.

Ziel:
Nachweis der erfolgreichen CSV-Ingestion inklusive technischer Metadaten (`ingestion_ts`, `source_file`).

---

### 02_silver_preview.png
Darstellung der bereinigten und typisierten Silver-Tabellen.

Ziel:
Validierung von:
- Casting
- Deduplizierung mittels `ROW_NUMBER`
- Join-Logik zwischen Orders und Products

---

### 03_scd2_validation.png
Validierung der SCD2-Tabelle `silver_customers_scd2`.

Ziel:
- Kontrolle von `valid_from`
- Kontrolle von `valid_to`
- Prüfung von `is_current`
- Sicherstellung der Historisierung

---

## Gold Layer – Business KPIs

### 04_gold_daily_revenue.png
Aggregierter Tagesumsatz (`gold_daily_revenue`).

---

### 05_gold_product_revenue.png
Umsatz je Produkt (`gold_product_revenue`).

---

### 06_gold_top_customers.png
Top-Kunden nach Gesamtumsatz (`gold_top_customers`).

---

### 07_table_overview.png
Tabellenübersicht im Schema `retail_lakehouse`.

Ziel:
Nachweis der vollständigen Bronze-, Silver- und Gold-Implementierung.
