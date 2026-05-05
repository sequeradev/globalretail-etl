# Power BI Dashboard — Setup Guide

> ⚠️ **M0 Free Tier limitation**: MongoDB Atlas M0 does **not** support the
> MongoDB BI Connector (which exposes MongoDB as a SQL endpoint). The two
> realistic options for a student project are below. **Option A (Python
> script)** is the cleanest and the one we recommend.

---

## Option A — Connect via Python script (recommended)

### 1. Install the Python connector on the machine running Power BI Desktop

Power BI Desktop uses your local Python install. From a Windows terminal:

```powershell
py -m pip install pymongo pandas dnspython
```

Then in Power BI Desktop → **File → Options and settings → Options → Python scripting**
and point to the Python install that has these packages.

### 2. In Power BI Desktop: Get Data → Python script

Paste the script below, replacing the `MONGODB_URI` with the same one you use
in `.env` (read-only user is safer here):

```python
import pandas as pd
from pymongo import MongoClient

MONGODB_URI = "mongodb+srv://READONLY_USER:PASSWORD@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10_000)
coll = client["globalretail"]["fact_sales"]

# Projection keeps the payload small — only what the dashboard needs
projection = {
    "_id": 0,
    "invoice_no": 1, "stock_code": 1, "customer_id": 1,
    "invoice_date": 1, "quantity": 1, "unit_price_gbp": 1,
    "revenue_gbp": 1, "revenue_eur": 1,
    "country": 1, "region": 1, "population": 1,
}

fact_sales = pd.DataFrame(list(coll.find({}, projection)))
# Power BI handles tz-aware datetimes as local — convert explicitly
fact_sales["invoice_date"] = pd.to_datetime(fact_sales["invoice_date"]).dt.tz_convert("UTC").dt.tz_localize(None)
```

Power BI will import `fact_sales` as a table.

### 3. Schedule refresh

For an auto-refreshing dashboard you need:

* **Personal Gateway** installed on a machine that has the Python connector
* Or publish to Power BI Service with a personal gateway configured

For the capstone deliverable, a **manual refresh before the demo** is fine.

---

## Option B — Nightly export to Parquet/CSV

Add a fourth task to the DAG that dumps `fact_sales` to `include/exports/fact_sales.parquet`
after each successful load. Power BI can then use the **Folder** or **Parquet** connector
(no Mongo-specific driver needed). Simpler to schedule — but doesn't demonstrate the
Atlas integration the PDF asks for.

```python
@task(task_id="export_to_parquet")
def export_to_parquet() -> None:
    from pymongo import MongoClient
    import pandas as pd
    from etl.config import load_config

    cfg = load_config()
    with MongoClient(cfg.mongodb_uri) as client:
        df = pd.DataFrame(list(client[cfg.mongodb_database][cfg.mongodb_collection].find({}, {"_id": 0})))
    df.to_parquet("/opt/airflow/include/exports/fact_sales.parquet", index=False)
```

---

## Required visuals (from the PDF)

### 1. Total Revenue by Region — Bar Chart

| Field well | Value |
|---|---|
| Axis | `region` |
| Values | `revenue_eur` (Sum) |

Sort descending by value. Add data labels. Use the default categorical palette.

### 2. Revenue Trends over Months — Line Chart

Create a **Month** calculated column first:

```
Month = FORMAT('fact_sales'[invoice_date], "yyyy-MM")
```

| Field well | Value |
|---|---|
| Axis | `Month` (sorted ascending) |
| Values | `revenue_eur` (Sum) |

Optionally add a `region` field to the **Legend** for a stacked view.

### 3. High-Value Customer Analysis — Scatter Plot

Create an **aggregated customer view** using a measure table:

```
Orders = DISTINCTCOUNT('fact_sales'[invoice_no])
Revenue EUR = SUM('fact_sales'[revenue_eur])
```

| Field well | Value |
|---|---|
| Details | `customer_id` |
| X Axis | `Orders` |
| Y Axis | `Revenue EUR` |
| Size (optional) | `Revenue EUR` |

Filter out `customer_id = "nan"` if any NaNs slipped through. Consider a
Top N filter (top 200 customers by revenue) to keep the chart readable.

---

## Suggested layout

```
┌────────────────────────────────────┬──────────────────────────────┐
│                                    │                              │
│   Revenue Trends over Months       │   Total Revenue by Region    │
│        (Line Chart)                │        (Bar Chart)           │
│                                    │                              │
├────────────────────────────────────┴──────────────────────────────┤
│                                                                   │
│          High-Value Customer Analysis (Scatter Plot)              │
│                    Orders vs. Revenue EUR                         │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

Add slicers for `region` and a date range at the top for interactivity.
