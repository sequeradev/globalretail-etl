"""Extract stage.

Responsibilities:
    1. Read the watermark from MongoDB (``_pipeline_metadata``).
    2. Read the source CSV in chunks, keeping only rows with InvoiceDate > watermark.
    3. Fetch reference data from REST Countries (region, population).
    4. Fetch historical GBP->EUR rates from Frankfurter, one per invoice date,
       so revenue conversion is faithful to the period of the transaction.

All network calls are wrapped with exponential-backoff retries via tenacity.
External-data contracts (API shapes, minimum row counts) are validated
explicitly so a silent upstream change surfaces as a loud failure rather
than a corrupt downstream load.

Outputs are written to Parquet files in the staging directory and the task
returns a lightweight manifest (paths + counts) through XCom — never the data
itself.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
import requests
from pymongo import MongoClient
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from etl.config import Config, load_config
from etl.logger import get_logger, timed

log = get_logger("etl.extract")


# Sanity floors for upstream data: if REST Countries returns fewer than this
# many countries, something is clearly wrong with the API and we'd rather
# fail the run than silently lose region enrichment for most transactions.
MIN_COUNTRIES_EXPECTED = 100
MIN_FX_RATE = 0.5   # GBP/EUR sanity bounds — historic min ~1.04, max ~1.45
MAX_FX_RATE = 2.5


# ------------------------------------------------------------------------------
# Watermark
# ------------------------------------------------------------------------------
def get_last_watermark(cfg: Config) -> datetime | None:
    """Return the most recent InvoiceDate successfully loaded, or None if empty."""
    with MongoClient(cfg.mongodb_uri, serverSelectionTimeoutMS=10_000) as client:
        meta = client[cfg.mongodb_database][cfg.metadata_collection]
        doc = meta.find_one({"_id": "last_watermark"})
        if not doc:
            return None
        return doc["invoice_date"]


# ------------------------------------------------------------------------------
# CSV reader (incremental, chunked)
# ------------------------------------------------------------------------------
def read_sales_csv(cfg: Config, watermark: datetime | None) -> pd.DataFrame:
    """Read the Online Retail CSV in chunks, keeping only rows newer than ``watermark``.

    Reading in chunks means peak memory is bounded regardless of source size,
    and on incremental runs we only materialize the rows we actually need.
    The Kaggle dataset is ISO-8859-1 (utf-8 will fail) and CustomerID has NaNs
    (so it must be a string dtype, not int).
    """
    if not cfg.csv_path.exists():
        raise FileNotFoundError(
            f"CSV not found at {cfg.csv_path}. "
            "Place the Kaggle Online Retail file there (see README)."
        )

    dtype_map = {
        "InvoiceNo": "string",
        "StockCode": "string",
        "Description": "string",
        "Quantity": "int64",
        "UnitPrice": "float64",
        "CustomerID": "string",
        "Country": "string",
    }

    chunks: list[pd.DataFrame] = []
    rows_scanned = 0
    for chunk in pd.read_csv(
        cfg.csv_path,
        encoding="ISO-8859-1",
        dtype=dtype_map,
        parse_dates=["InvoiceDate"],
        chunksize=cfg.csv_chunksize,
    ):
        rows_scanned += len(chunk)
        if chunk["InvoiceDate"].dt.tz is None:
            chunk["InvoiceDate"] = chunk["InvoiceDate"].dt.tz_localize("UTC")
        if watermark is not None:
            wm = watermark if watermark.tzinfo is not None else watermark.replace(tzinfo=timezone.utc)
            chunk = chunk[chunk["InvoiceDate"] > wm]
        if not chunk.empty:
            chunks.append(chunk)

    df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=list(dtype_map))

    if watermark is None:
        log.info("full_load_no_watermark", extra={"rows": len(df), "rows_scanned": rows_scanned})
    else:
        log.info(
            "incremental_filter_applied",
            extra={
                "watermark": watermark.isoformat(),
                "rows_scanned": rows_scanned,
                "rows_kept": len(df),
            },
        )
    return df


# ------------------------------------------------------------------------------
# API fetchers (with retry + schema validation)
# ------------------------------------------------------------------------------
_RETRY = dict(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=16),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
    before_sleep=before_sleep_log(log, 30),
)


@retry(**_RETRY)
def fetch_countries(cfg: Config) -> pd.DataFrame:
    """Fetch region + population for every country from REST Countries.

    Validates the response shape and minimum row count so a breaking API change
    (renamed field, partial outage) fails the task instead of silently producing
    an empty enrichment table.
    """
    log.info("api_call", extra={"api": "restcountries"})
    resp = requests.get(cfg.countries_api_url, timeout=cfg.api_timeout_seconds)
    resp.raise_for_status()
    data = resp.json()

    if not isinstance(data, list):
        raise ValueError(f"REST Countries returned non-list payload: {type(data).__name__}")

    records: list[dict[str, Any]] = []
    skipped = 0
    for c in data:
        name = (c.get("name") or {}).get("common")
        if not name:
            skipped += 1
            continue
        records.append(
            {
                "country": name,
                "region": c.get("region", "Unknown"),
                "population": int(c.get("population", 0)),
            }
        )

    df = pd.DataFrame.from_records(records)

    if len(df) < MIN_COUNTRIES_EXPECTED:
        raise ValueError(
            f"REST Countries returned only {len(df)} countries "
            f"(expected at least {MIN_COUNTRIES_EXPECTED}). "
            "Possible API breaking change — refusing to load with degraded enrichment."
        )

    log.info(
        "api_response",
        extra={"api": "restcountries", "rows": len(df), "skipped_no_name": skipped},
    )
    return df


@retry(**_RETRY)
def fetch_exchange_rate_for_date(cfg: Config, on_date: date) -> float:
    """Fetch the GBP -> EUR rate as of a specific historical date.

    Frankfurter's ``/v1/{YYYY-MM-DD}`` endpoint returns the rate for that date
    (or the closest preceding business day for weekends/holidays). Using a
    historical rate per transaction gives faithful revenue_eur figures rather
    than applying today's rate to 2011 invoices.
    """
    url = f"{cfg.frankfurter_api_url.rstrip('/')}/{on_date.isoformat()}"
    resp = requests.get(
        url,
        params={"base": cfg.base_currency, "symbols": cfg.target_currency},
        timeout=cfg.api_timeout_seconds,
    )
    resp.raise_for_status()
    payload = resp.json()
    rate = float(payload["rates"][cfg.target_currency])
    if not (MIN_FX_RATE <= rate <= MAX_FX_RATE):
        raise ValueError(
            f"FX rate {rate} for {on_date} outside sanity bounds "
            f"[{MIN_FX_RATE}, {MAX_FX_RATE}] — possible API regression."
        )
    return rate


def fetch_fx_rates_for_dates(cfg: Config, dates: list[date]) -> dict[str, float]:
    """Fetch GBP->EUR rates for a set of unique dates and return a date-keyed dict.

    Keys are ISO date strings so the dict survives a Parquet round-trip without
    needing to handle Python ``date`` objects in the manifest.
    """
    rates: dict[str, float] = {}
    log.info("api_call", extra={"api": "frankfurter", "unique_dates": len(dates)})
    for d in dates:
        rates[d.isoformat()] = fetch_exchange_rate_for_date(cfg, d)
    log.info(
        "api_response",
        extra={
            "api": "frankfurter",
            "rates_fetched": len(rates),
            "min_rate": min(rates.values()) if rates else None,
            "max_rate": max(rates.values()) if rates else None,
        },
    )
    return rates


# ------------------------------------------------------------------------------
# Airflow-facing entrypoint
# ------------------------------------------------------------------------------
@timed("extract")
def run_extract() -> dict[str, Any]:
    """Orchestrate the extract stage and write staging Parquet files."""
    cfg = load_config()
    cfg.staging_dir.mkdir(parents=True, exist_ok=True)

    watermark = get_last_watermark(cfg)
    sales_df = read_sales_csv(cfg, watermark)
    countries_df = fetch_countries(cfg)

    # Fetch one FX rate per unique invoice date so the conversion respects
    # the historical rate of each transaction, not today's spot rate.
    if sales_df.empty:
        fx_rates: dict[str, float] = {}
    else:
        unique_dates = sorted({d.date() for d in sales_df["InvoiceDate"]})
        fx_rates = fetch_fx_rates_for_dates(cfg, unique_dates)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    sales_path = cfg.staging_dir / f"sales_raw_{run_ts}.parquet"
    countries_path = cfg.staging_dir / f"countries_{run_ts}.parquet"
    fx_path = cfg.staging_dir / f"fx_rates_{run_ts}.parquet"

    sales_df.to_parquet(sales_path, index=False)
    countries_df.to_parquet(countries_path, index=False)
    pd.DataFrame(
        {"date": list(fx_rates.keys()), "fx_rate_gbp_eur": list(fx_rates.values())}
    ).to_parquet(fx_path, index=False)

    return {
        "records": int(len(sales_df)),
        "sales_parquet": str(sales_path),
        "countries_parquet": str(countries_path),
        "fx_parquet": str(fx_path),
        "watermark": watermark.isoformat() if watermark else None,
    }
