# GlobalRetail ETL — Capstone Project

Incremental ETL pipeline consolidating Online Retail sales data with country
metadata (REST Countries API) and currency exchange rates (Frankfurter API)
into **MongoDB Atlas**, orchestrated by **Apache Airflow 2.9** on Docker.

```
online_retail.csv  ─┐
REST Countries API ─┼─► Airflow (extract → transform → load) ─► MongoDB Atlas ─► Power BI
Frankfurter API    ─┘
```

---

## 1. Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Desktop | 4.x+ | Must have at least 4 GB RAM and 2 CPUs allocated |
| Docker Compose | v2 | Bundled with Docker Desktop |
| MongoDB Atlas | M0 free tier | Create at https://cloud.mongodb.com |
| Power BI Desktop | latest | Windows only — for the dashboard deliverable |

### Dataset

Download **Online Retail** from Kaggle (or UCI):

* Kaggle: `https://www.kaggle.com/datasets/carrie1/ecommerce-data`
* Save the file as `include/data/online_retail.csv` in this repo.

The file is git-ignored by default.

---

## 2. MongoDB Atlas setup (5 min)

1. Create a free **M0 cluster** (any region close to you).
2. **Database Access** → add a user with `readWrite` on the `globalretail` database
   (or `Atlas admin` for simplicity in a school project).
3. **Network Access** → add your current IP, or `0.0.0.0/0` (public; fine for a
   student project, **never** do this in production).
4. Copy the **connection string** (`mongodb+srv://...`) — you'll paste it into `.env` next.

---

## 3. Local setup

```bash
# Clone / unzip the project, then:
cp .env.example .env
# Edit .env and paste your MONGODB_URI. On Linux also set AIRFLOW_UID:
#   echo "AIRFLOW_UID=$(id -u)" >> .env

# Build the custom Airflow image (includes pymongo, pandas, etc.)
docker compose build

# One-time: run DB migrations and create the admin user
docker compose up airflow-init

# Start Airflow (webserver + scheduler + postgres)
docker compose up -d

# Check everything is healthy
docker compose ps
```

Open **http://localhost:8080** → log in with `admin / admin` (or the values you set).

You should see a DAG called **`globalretail_etl`** in the UI. Toggle it **on**
and hit the ▶ "Trigger DAG" button.

---

## 4. What the DAG does

```
┌─────────────┐    ┌─────────────────┐    ┌────────────────┐
│ extract_data│ ─► │ transform_data  │ ─► │ load_to_mongo  │
└─────────────┘    └─────────────────┘    └────────────────┘
```

1. **extract_data**
   * reads the watermark from `_pipeline_metadata` in Mongo
   * filters the CSV to rows with `InvoiceDate > watermark`
   * fetches REST Countries + Frankfurter with exponential-backoff retries
   * writes `sales_raw_*.parquet` and `countries_*.parquet` to staging

2. **transform_data**
   * cleans: drops rows with missing CustomerID, Quantity ≤ 0, UnitPrice ≤ 0
   * normalizes: lowercases and strips whitespace on Country / CustomerID
   * enriches: joins with REST Countries to add `region` and `population`
   * converts: computes `revenue_gbp` and `revenue_eur` using the FX rate
   * writes `fact_sales_*.parquet` to staging

3. **load_to_mongo**
   * ensures the unique index on `(invoice_no, stock_code, customer_id)`
   * uses `bulk_write` with `UpdateOne(..., upsert=True)` in batches of 1000
   * advances the watermark to `max(invoice_date)` last loaded

The whole flow is **idempotent**: re-running it after any kind of failure
produces the exact same final state in MongoDB.

---

## 5. Running tests

```bash
docker compose run --rm airflow-scheduler bash -lc "cd /opt/airflow && pytest tests/ -v"
```

7 unit tests cover cleaning, normalization, enrichment, FX math and business-key stability.

---

## 6. Troubleshooting

| Symptom | Fix |
|---|---|
| `airflow-init` exits with permission errors on Linux | Set `AIRFLOW_UID=$(id -u)` in `.env` and re-run |
| `ServerSelectionTimeoutError` when loading | Your IP is not whitelisted in Atlas, or the URI is wrong |
| `FileNotFoundError: online_retail.csv` | Put the Kaggle CSV at `include/data/online_retail.csv` |
| DAG not visible in the UI | Check scheduler logs: `docker compose logs -f airflow-scheduler` |
| `BulkWriteError: duplicate key` on first run | You already have data without the unique index. Drop the collection and rerun |
| Want to reset the watermark | `db._pipeline_metadata.deleteOne({_id: "last_watermark"})` in Atlas |

---

## 7. Deliverables checklist

* [x] `dags/globalretail_etl_dag.py` — Airflow DAG (`extract_data >> transform_data >> load_to_mongo`)
* [x] Incremental extraction via watermark
* [x] Batch loading of 1000 records
* [x] Idempotency via unique compound index + `bulk_write(upsert=True)`
* [x] Structured logging (JSON) with timing per task
* [x] Retries with exponential backoff at two layers (tenacity + Airflow)
* [x] Unit tests for all business rules
* [ ] Power BI dashboard — see `docs/powerbi_setup.md`
* [x] Technical report — see `docs/technical_report.md`

---

## 8. Project structure

```
globalretail-etl/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── README.md
├── dags/
│   ├── globalretail_etl_dag.py       # the DAG
│   └── etl/
│       ├── config.py                 # env-driven config
│       ├── logger.py                 # JSON logger + @timed
│       ├── extract.py                # CSV + API fetchers (with retry)
│       ├── transform.py              # cleaning / enrichment / FX
│       └── load.py                   # idempotent upserts to Mongo
├── tests/
│   └── test_transform.py             # 7 pytest cases
├── include/
│   ├── data/online_retail.csv        # YOU place this file here
│   └── staging/                      # Parquet intermediates (auto-created)
└── docs/
    ├── architecture.md
    ├── powerbi_setup.md
    └── technical_report.md
```
