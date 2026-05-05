# Architecture

## High-level flow

```mermaid
flowchart LR
    subgraph Sources["Sources"]
        CSV["online_retail.csv"]
        API1["REST Countries API"]
        API2["Frankfurter API"]
    end

    subgraph Airflow["Airflow 2.9 (Docker)"]
        direction TB
        EXT["extract_data"]
        TRF["transform_data"]
        LOAD["load_to_mongo"]
        EXT --> TRF --> LOAD
    end

    subgraph Staging["Shared volume: include/staging/"]
        PQ["*.parquet"]
    end

    subgraph Mongo["MongoDB Atlas M0"]
        FACT["fact_sales"]
        META["_pipeline_metadata"]
    end

    CSV --> EXT
    API1 --> EXT
    API2 --> EXT
    EXT --> PQ
    PQ --> TRF
    TRF --> PQ
    PQ --> LOAD
    LOAD --> FACT
    LOAD --> META
    META -.read watermark.-> EXT
    FACT --> PowerBI["Power BI"]
```

## Sequence diagram — one DAG run

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant E as extract_data
    participant Mongo as MongoDB Atlas
    participant T as transform_data
    participant L as load_to_mongo

    S->>E: trigger
    E->>Mongo: find_one("last_watermark")
    Mongo-->>E: 2011-09-15T00:00:00Z
    E->>E: read CSV → filter > watermark
    E->>E: GET restcountries.com (retry)
    E->>E: GET api.frankfurter.dev (retry)
    E-->>S: manifest{sales_parquet, countries_parquet, fx_rate}
    S->>T: trigger with manifest
    T->>T: clean → normalize → enrich → apply_fx
    T-->>S: manifest{fact_parquet}
    S->>L: trigger with manifest
    L->>Mongo: ensure_indexes
    loop batches of 1000
        L->>Mongo: bulk_write(UpdateOne upsert=True)
        Mongo-->>L: BulkWriteResult
    end
    L->>Mongo: update_one("last_watermark", {$set:{invoice_date: max}}, upsert=True)
    L-->>S: done
```

## Data contracts

### `fact_sales` document shape

```json
{
  "_id": "ObjectId(...)",
  "invoice_no": "536365",
  "stock_code": "85123a",
  "description": "white hanging heart t-light holder",
  "quantity": 6,
  "unit_price_gbp": 2.55,
  "invoice_date": "2010-12-01T08:26:00Z",
  "customer_id": "17850",
  "country": "united kingdom",
  "region": "Europe",
  "population": 67886011,
  "revenue_gbp": 15.30,
  "revenue_eur": 17.90,
  "fx_rate_gbp_eur": 1.170
}
```

### `_pipeline_metadata` document shape

```json
{
  "_id": "last_watermark",
  "invoice_date": "2011-12-09T12:50:00Z",
  "updated_at": "2024-06-01T02:00:37.125Z"
}
```

### Indexes on `fact_sales`

| Name | Fields | Purpose |
|---|---|---|
| `uq_business_key` (unique) | `invoice_no`, `stock_code`, `customer_id` | idempotent upserts |
| `ix_invoice_date` | `invoice_date` | watermark queries, Power BI time filter |
| `ix_region` | `region` | bar chart aggregation |
