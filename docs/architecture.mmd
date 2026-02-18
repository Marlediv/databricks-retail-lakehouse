```mermaid
flowchart LR
    S[Source CSV in Unity Catalog Volume\ncustomers.csv, products.csv, orders.csv]

    subgraph Bronze[Bronze Layer - Delta]
      B1[bronze_customers\n+ ingestion_ts, source_file]
      B2[bronze_products\n+ ingestion_ts, source_file]
      B3[bronze_orders\n+ ingestion_ts, source_file]
    end

    subgraph Silver[Silver Layer - Typed + Deduplicated]
      S1[silver_customers_current\nROW_NUMBER by customer_id + updated_at DESC]
      S2[silver_products\nTyped product attributes]
      S3[silver_order_lines\norders + products + line_revenue]
      S4[silver_customers_scd2\nvalid_from, valid_to, is_current, record_hash]
    end

    subgraph Gold[Gold Layer - KPI Tables]
      G1[gold_daily_revenue]
      G2[gold_product_revenue]
      G3[gold_top_customers]
    end

    V[Validation Queries\nSHOW TABLES, counts, dedupe checks]

    S --> B1
    S --> B2
    S --> B3

    B1 --> S1
    B2 --> S2
    B3 --> S3
    S2 --> S3
    S1 --> S4

    S3 --> G1
    S3 --> G2
    S3 --> G3
    S1 --> G3

    G1 --> V
    G2 --> V
    G3 --> V
    S4 --> V
```
