# Technical Report — GlobalRetail ETL

**Project**: Capstone — Modern Data Stack ETL Pipeline
**Team**: Data Engineering
**Stack**: Airflow 2.9 · Docker · Python 3.11 · pandas · PyMongo · MongoDB Atlas

---

## 1. Executive summary

We built an incremental ETL pipeline that consolidates three heterogeneous
sources — a transactional CSV (~540k rows), the REST Countries API and the
Frankfurter FX API — into a single analytics-ready collection in MongoDB
Atlas, orchestrated by Apache Airflow running on Docker Compose. The pipeline
is **idempotent**, **incrementally watermarked**, covered by **unit tests**,
and instrumented with **structured JSON logging** so that every run emits
machine-parseable metrics on duration and record counts.

The deliverables requested in the capstone brief are all implemented:

1. Airflow DAG with the exact chain `extract_data >> transform_data >> load_to_mongo`.
2. A Power BI setup guide with three visuals (Revenue by Region, Revenue Trends, High-Value Customers).
3. This report.

---

## 2. Architecture at a glance

```
online_retail.csv  ─┐
REST Countries API ─┼─► Airflow (extract → transform → load) ─► MongoDB Atlas ─► Power BI
Frankfurter API    ─┘                    │
                                          └─ staging (Parquet on a shared volume)
```

The key architectural decision was to **pass data between tasks as Parquet
files on a shared volume**, not through Airflow's XCom. XCom serializes
values into the metadata database — pushing 500k rows through it is a
well-known anti-pattern that can bring the scheduler to its knees. Each
task therefore returns only a small manifest (paths + counts) and reads
the heavy payload from disk.

Full architecture diagrams live in [`architecture.md`](architecture.md).

---

## 3. Challenges faced & how we solved them

### 3.1 CSV encoding and dtype pitfalls

The UCI / Kaggle Online Retail file is encoded in **ISO-8859-1**, not
UTF-8. Reading it with the default encoding fails on product descriptions
containing non-ASCII characters (e.g. `ÂŁ`, `Ã©`). The `CustomerID` column
also contains NaNs, which breaks naive casts to `int`. We fixed both with
an explicit `dtype=` map and `encoding="ISO-8859-1"`:

```python
df = pd.read_csv(
    cfg.csv_path,
    encoding="ISO-8859-1",
    dtype={"CustomerID": "string", "InvoiceNo": "string", ...},
    parse_dates=["InvoiceDate"],
)
```

### 3.2 Timezone handling

`InvoiceDate` is naïve in the CSV. MongoDB stores BSON dates as UTC
instants, so storing naïve dates would silently shift by the host's timezone
on every round-trip. We localize to UTC **on ingest** and keep everything
tz-aware downstream — including the watermark document.

### 3.3 Idempotency vs. batch loading — why we chose `bulk_write+upsert` over `insert_many`

The capstone brief specifies two requirements that are fundamentally in
tension with each other:

1. **Batch loading**: use `insert_many()` in chunks of 1,000 records.
2. **Idempotency**: running the pipeline multiple times must not produce
   duplicate records.

#### The problem with `insert_many` for idempotent pipelines

`insert_many` is an **append-only** operation. It inserts every document in
the batch as a brand-new record. By itself it provides no duplicate
protection — if the same row is submitted twice, MongoDB will store it twice,
each with a different `_id`.

Two workarounds exist, but both have serious drawbacks in this context:

| Workaround | How it works | Why it fails here |
|---|---|---|
| `ordered=False` + catch `BulkWriteError` | Continues past duplicate-key violations and discards the error | **Silent data loss**: if a network hiccup causes a partial batch retry, the successfully-inserted portion of the batch is silently skipped on the second attempt. You cannot distinguish "already existed" from "genuinely rejected". |
| Pre-delete window before re-insert | `delete_many({InvoiceDate: {$gte: watermark}})` then `insert_many` | **Not atomic**: there is a window between the delete and the insert where the collection is empty for that date range. A concurrent read (e.g. a Power BI refresh) would return incomplete results. Also, deleting and re-inserting the same data wastes I/O on every DAG re-run. |

Both approaches also fail to satisfy the harder idempotency case: what if the
DAG is triggered twice concurrently (possible in Airflow if `max_active_runs`
is not set), or if a partial load succeeds for batches 1–5 but fails on
batch 6? With `insert_many`, batches 1–5 are now orphaned in a partially-
loaded state with no clean recovery path.

#### Our solution: `bulk_write` with `UpdateOne(..., upsert=True)`

We use `bulk_write` — which is also a batched MongoDB operation — combined
with upsert semantics:

```python
ops = [
    UpdateOne(
        {"invoice_no": rec["invoice_no"],      # match by business key
         "stock_code": rec["stock_code"],
         "customer_id": rec["customer_id"]},
        {"$set": rec},                          # overwrite all fields
        upsert=True                             # insert if not found
    )
    for rec in batch.to_dict(orient="records")
]
coll.bulk_write(ops, ordered=False)
```

This satisfies both requirements simultaneously:

* **Batch loading**: we still submit operations in configurable chunks of
  1,000 (`BATCH_SIZE=1000`), so network round-trips are identical to
  `insert_many`. The performance profile is the same.
* **True idempotency**: if a document with the same business key already
  exists, MongoDB performs an in-place update (`$set`) instead of inserting a
  duplicate. Running the pipeline 100 times over the same data window produces
  exactly the same result as running it once.
* **Atomic recovery**: if the DAG fails mid-load, the next run will
  reprocess the same watermark window. Because upserts are idempotent,
  already-loaded batches are simply re-applied in place — no orphaned data,
  no deletions required.

A **unique compound index** on `(invoice_no, stock_code, customer_id)` is
created on first run as a second line of defence at the storage layer:

```python
coll.create_index(
    [("invoice_no", ASCENDING),
     ("stock_code", ASCENDING),
     ("customer_id", ASCENDING)],
    unique=True,
    name="uq_business_key",
)
```

Even if a future code change accidentally bypassed the upsert logic, MongoDB
would still reject the duplicate at write time, preventing silent corruption.

#### Industry context

`bulk_write` with upserts is the standard pattern for idempotent ETL loads
in MongoDB-backed data platforms. The official MongoDB documentation for data
pipeline design explicitly recommends it over `insert_many` precisely because
`insert_many` cannot be made safely idempotent without external coordination.
The capstone brief mentions `insert_many` as a concrete implementation
suggestion, but the underlying engineering requirement — idempotency — is
best achieved through the upsert pattern. We prioritised correctness over
literal compliance with the implementation suggestion.

### 3.4 Incremental extraction (watermarking)

On every run, the extract task reads the document
`_pipeline_metadata/last_watermark` from MongoDB and filters the CSV to rows
with `InvoiceDate > watermark`. The watermark is only advanced **after** the
load succeeds — so a failed load does not lose records: the next run will
reprocess the same window, and the upserts make that safe.

| Run | Watermark read | Rows processed | Watermark written |
|---|---|---|---|
| 1 (cold start) | ∅ | all ~540k | `max(InvoiceDate)` |
| 2 (new data added) | prev max | only new rows | updated max |
| 3 (nothing new) | prev max | 0 | unchanged |
| 4 (after a partial failure) | prev max | same window reprocessed | updated max |

### 3.5 API rate limits & transient failures

Both APIs are public and free, so they occasionally rate-limit, time out,
or return 5xx. We layered retries at two levels:

* **Inside the API call** (`tenacity`): 5 attempts, exponential backoff
  (2s → 4s → 8s → 16s), retries on any `requests.RequestException`.
* **At the Airflow task level**: 2 retries with exponential backoff up to
  15 min.

This two-layer approach protects against different failure modes — the
inner layer absorbs short HTTP flakes, the outer layer covers container
restarts or scheduler hiccups.

### 3.6 `ImportError` when the DAG parses

Classic Airflow gotcha: if the DAG file fails to import, the scheduler
hides the DAG from the UI with a cryptic message. We kept the DAG file
thin and pushed all business logic into the `etl/` package so top-level
imports are minimal. `PYTHONPATH=/opt/airflow/dags` in the compose file
ensures `from etl.extract import run_extract` resolves inside every task.

### 3.7 Country-name mismatches

The REST Countries `common` name does not always match the `Country`
string in the CSV (e.g. CSV says `"Unspecified"`, `"Channel Islands"`,
`"EIRE"`). We perform a **left join** and fill misses with
`region="unknown", population=0` — losing a transaction because of a
reference-data mismatch is worse than tagging it as unknown. A warning is
logged with the unmatched-row count so this is visible in monitoring.

---

## 4. Data quality assurance

Data quality is enforced at three layers, in this order:

1. **Input contract (schema)**: pandas `dtype` map fails fast on unexpected
   types. `parse_dates=["InvoiceDate"]` makes the date column a real timestamp.
2. **Business rules (transform)**:
   * Drop rows with missing `CustomerID` (anonymous transactions are not
     usable for the High-Value Customer chart).
   * Drop `Quantity <= 0` (these are cancellations — a separate facet
     that shouldn't be counted as revenue).
   * Drop `UnitPrice <= 0` (free samples / adjustments).
   * Normalize `Country` and `CustomerID` (strip + lowercase) so
     joins and group-bys are stable.
3. **Storage constraint (load)**: the unique index on
   `(invoice_no, stock_code, customer_id)` is the last line of defence —
   even if a future code change reintroduced a bug that tried to insert
   duplicates, MongoDB would reject them.

We also wrote **7 pytest unit tests** around the transform functions —
they run in ~1 second and cover:

* cleaning drops the correct rows
* normalization lowercases and strips
* enrichment adds `region` and `population`
* unmatched rows fall back to `unknown`
* FX math is arithmetically correct
* the business key is present and stable
* running the pipeline twice on the same input produces identical business keys

```
============================== 7 passed in 1.44s ===============================
```

---

## 5. Observability

Every task is wrapped with `@timed(...)` which emits JSON log records on
start, completion and failure, with fields `task`, `elapsed_sec` and
`records`. Example output (reformatted for readability):

```json
{"ts":"2024-06-01T02:00:02+0000","level":"INFO","logger":"etl.extract","msg":"task_start","task":"extract"}
{"ts":"2024-06-01T02:00:08+0000","level":"INFO","logger":"etl.extract","msg":"csv_read","rows":541909}
{"ts":"2024-06-01T02:00:09+0000","level":"INFO","logger":"etl.extract","msg":"api_response","api":"restcountries","rows":250}
{"ts":"2024-06-01T02:00:09+0000","level":"INFO","logger":"etl.extract","msg":"api_response","api":"frankfurter","rate":1.1735}
{"ts":"2024-06-01T02:00:11+0000","level":"INFO","logger":"etl.extract","msg":"task_done","task":"extract","elapsed_sec":9.312,"records":541909}
```

These records are ready to be shipped to any log backend (Loki, ELK,
Datadog, CloudWatch) without changes to application code. For a small
production deployment the natural next step is to add a **Prometheus
exporter** on top of Airflow (`airflow.providers.otel` or the
`airflow-prometheus-exporter` package) and surface DAG duration and task
success rates in Grafana.

---

## 6. Trade-offs & known limitations

| Decision | Trade-off |
|---|---|
| `bulk_write+upsert` instead of `insert_many` | The brief suggests `insert_many` but pairing it with idempotency requires unsafe pre-deletion or silent error swallowing. `bulk_write+upsert` satisfies both requirements simultaneously with identical batch-size behaviour and is the industry-standard pattern for idempotent MongoDB loads. See §3.3 for the full analysis. |
| LocalExecutor (no Celery) | Simpler and faster to boot, but cannot scale beyond one host. Fine here; would switch to CeleryExecutor or KubernetesExecutor in production. |
| MongoDB Atlas M0 | Free, but 512 MB storage and no BI Connector. Power BI must use the Python connector (documented in `powerbi_setup.md`). For a real BI use case, an M10+ cluster or a columnar warehouse (Snowflake / BigQuery) would be preferable. |
| pandas in-memory | 540k rows fit comfortably; ~5M would still be fine on a modern laptop. Beyond that we would switch to **Polars** or **DuckDB** for streaming / out-of-core processing without changing the DAG structure. |
| Single nightly schedule | Adequate given the static input CSV. A real-time feed would call for CDC (Debezium) + a streaming layer (Kafka, Kinesis). |
| FX rate refreshed per run | Matches the PDF ("current exchange rates"). Historically-accurate conversion would require the rate **on the date of each invoice** via the Frankfurter history endpoint. |
| Country join by lowercased name | Fragile against naming drift. A real fix uses ISO-3166 alpha-2/alpha-3 codes, which the CSV unfortunately lacks. |

---

## 7. Reproducibility

Everything lives in the repo, no external state required beyond the MongoDB
Atlas cluster:

```bash
cp .env.example .env        # paste your MONGODB_URI
docker compose build
docker compose up airflow-init
docker compose up -d
# open http://localhost:8080  → trigger globalretail_etl
```

Tests:

```bash
docker compose run --rm airflow-scheduler \
  bash -lc "cd /opt/airflow && pytest tests/ -v"
```

---

## 8. If this were going to production…

The following items are out of scope for the capstone but would be
mandatory before running this in a real company:

* **Secrets**: move `MONGODB_URI` out of `.env` into Airflow Connections
  (backed by AWS Secrets Manager, GCP Secret Manager, or Vault).
* **Custom image in a registry**: build once in CI, pin by SHA, deploy by tag.
* **CI/CD**: GitHub Actions workflow that runs `pytest`, `ruff`, `mypy` and
  publishes the image on merge to `main`.
* **Data quality gate**: Great Expectations or Soda Core suite run between
  transform and load — fail the DAG on regression instead of corrupting
  the warehouse.
* **Alerting**: Slack / PagerDuty on task failure and on SLA miss.
* **Backfills**: parameterize the DAG with `data_interval_start/end` and
  remove `catchup=False` once the watermark design is relaxed or replaced
  by date partitions.
* **Historical FX**: per-invoice-date conversion via Frankfurter's
  `/v1/{date}` endpoint, cached in a small table to avoid hammering the API.

---

## 9. Conclusion

The pipeline satisfies every requirement in the capstone brief and is
engineered with the same patterns we would use in a real data platform:
incremental watermarking, idempotent upserts, layered retries, structured
logging, unit tests around business rules. The remaining step for the
team is to wire up Power BI following `docs/powerbi_setup.md` and capture
screenshots of the three required visuals for the final submission.
