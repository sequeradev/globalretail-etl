"""Transform stage.

All business rules described in the capstone PDF live here, in pure
pandas — which makes them trivial to unit-test without Airflow or Mongo.

Cleaning rules:
    * drop rows with missing CustomerID
    * drop rows with Quantity <= 0   (cancellations are negative)
    * drop rows with UnitPrice <= 0

Normalization:
    * Country and CustomerID lowercased + whitespace-stripped
    * a `country_key` column derived from Country is the join key

Enrichment:
    * left-join with REST Countries to add `region` and `population`
    * rows whose country does not match fall back to region="unknown",
      population=0 -- we never silently drop them

Monetary:
    * revenue_gbp = Quantity * UnitPrice  (computed before FX)
    * revenue_eur = revenue_gbp * fx_rate_for_invoice_date  (historical, not spot)

Idempotency:
    * The business key includes a SHA1 of the full record so that two line
      items in the same invoice for the same product (rare but possible in the
      source) do not collide and silently overwrite each other on upsert.

Data Quality:
    * Every transform step records its input/output counts. The function
      ``build_quality_report`` aggregates them into a single dict that is
      logged at task end and surfaced via XCom for downstream observability.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from etl.config import load_config
from etl.logger import get_logger, timed

log = get_logger("etl.transform")

# ------------------------------------------------------------------------------
# Country name aliases
#
# The Online Retail CSV uses several non-standard country names that do not
# match the "common" names returned by REST Countries v3.1.  This mapping is
# applied *before* the country_key join so those rows receive a real region
# instead of falling back to "unknown".
#
# Verified against REST Countries v3.1 /all?fields=name (May 2026).
# "Unspecified" and "European Community" have no valid single-country match
# and are intentionally left out -- they remain region="unknown".
# ------------------------------------------------------------------------------
COUNTRY_ALIASES: dict[str, str] = {
    "eire": "ireland",             # Irish name for Ireland
    "channel islands": "jersey",   # British Crown Dependency; largest island
    "usa": "united states",        # CSV abbreviation
    "rsa": "south africa",         # CSV abbreviation
    "czech republic": "czechia",   # REST Countries v3 renamed it
}


# ------------------------------------------------------------------------------
# Pure functions -- easy to unit-test
# ------------------------------------------------------------------------------
def clean_sales(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Drop rows violating the data-quality contract.

    Returns the cleaned frame plus a per-rule drop counter so the caller can
    build a data-quality report rather than only logging totals.
    """
    rows_in = len(df)
    after_id = df.dropna(subset=["CustomerID"])
    dropped_id = rows_in - len(after_id)

    after_qty = after_id[after_id["Quantity"] > 0]
    dropped_qty = len(after_id) - len(after_qty)

    after_price = after_qty[after_qty["UnitPrice"] > 0]
    dropped_price = len(after_qty) - len(after_price)

    counters = {
        "rows_in": rows_in,
        "dropped_missing_customer_id": dropped_id,
        "dropped_non_positive_quantity": dropped_qty,
        "dropped_non_positive_price": dropped_price,
        "rows_out": len(after_price),
    }
    log.info("cleaning_applied", extra=counters)
    return after_price.copy(), counters


def normalize_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and strip whitespace on text fields.

    Applies COUNTRY_ALIASES after lowercasing so non-standard names in the
    source CSV (e.g. "EIRE", "USA", "RSA") resolve to the API canonical form
    before the enrichment join.
    """
    df = df.copy()
    df["Country"] = df["Country"].str.strip().str.lower()
    df["CustomerID"] = df["CustomerID"].str.strip().str.lower()
    df["country_key"] = df["Country"].map(lambda x: COUNTRY_ALIASES.get(x, x))
    return df


def normalize_countries(df: pd.DataFrame) -> pd.DataFrame:
    """Match the same key format used in normalize_sales."""
    df = df.copy()
    df["country_key"] = df["country"].str.strip().str.lower()
    return df[["country_key", "region", "population"]]


def enrich_with_countries(sales: pd.DataFrame, countries: pd.DataFrame) -> pd.DataFrame:
    """Left-join to avoid losing transactions whose country lacks a match."""
    merged = sales.merge(countries, on="country_key", how="left")
    unmatched = int(merged["region"].isna().sum())
    if unmatched:
        log.warning(
            "country_join_misses",
            extra={"unmatched_rows": unmatched, "total_rows": len(merged)},
        )
    merged["region"] = merged["region"].fillna("unknown")
    merged["population"] = merged["population"].fillna(0).astype("int64")
    return merged


def apply_fx_historical(df: pd.DataFrame, fx_rates: dict[str, float]) -> pd.DataFrame:
    """Apply per-date GBP->EUR rates so revenue reflects each invoice's period.

    For dates absent from the FX table (weekends, holidays, gaps), fall back
    to the nearest preceding rate. This matches Frankfurter's own behavior
    and avoids dropping otherwise-valid transactions over a calendar quirk.
    """
    df = df.copy()
    df["revenue_gbp"] = (df["Quantity"] * df["UnitPrice"]).round(4)

    fx_series = pd.Series(fx_rates, name="fx_rate_gbp_eur")
    fx_series.index = pd.to_datetime(fx_series.index, utc=True).normalize()
    fx_series = fx_series.sort_index()

    invoice_day = pd.to_datetime(df["InvoiceDate"], utc=True).dt.normalize()
    fx_aligned = fx_series.reindex(invoice_day.unique(), method="ffill")
    df["fx_rate_gbp_eur"] = invoice_day.map(fx_aligned).astype("float64")

    if df["fx_rate_gbp_eur"].isna().any():
        median_rate = float(fx_series.median())
        missing = int(df["fx_rate_gbp_eur"].isna().sum())
        log.warning(
            "fx_rate_fallback_used",
            extra={"missing_rows": missing, "fallback_rate": median_rate},
        )
        df["fx_rate_gbp_eur"] = df["fx_rate_gbp_eur"].fillna(median_rate)

    df["revenue_eur"] = (df["revenue_gbp"] * df["fx_rate_gbp_eur"]).round(4)
    return df


def _row_hash(row: pd.Series) -> str:
    """Stable short hash of the value-bearing fields."""
    payload = "|".join(
        str(row[c]) for c in ("InvoiceNo", "StockCode", "CustomerID", "Quantity", "UnitPrice", "InvoiceDate")
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]


def build_fact_records(df: pd.DataFrame) -> pd.DataFrame:
    """Shape the dataframe into the final fact-table schema we will load."""
    description = df["Description"] if "Description" in df.columns else pd.Series([""] * len(df))
    out = pd.DataFrame(
        {
            "invoice_no": df["InvoiceNo"],
            "stock_code": df["StockCode"],
            "description": description,
            "quantity": df["Quantity"].astype("int64"),
            "unit_price_gbp": df["UnitPrice"].astype("float64"),
            "invoice_date": df["InvoiceDate"],
            "customer_id": df["CustomerID"],
            "country": df["Country"],
            "region": df["region"],
            "population": df["population"],
            "revenue_gbp": df["revenue_gbp"],
            "revenue_eur": df["revenue_eur"],
            "fx_rate_gbp_eur": df["fx_rate_gbp_eur"],
        }
    )
    row_hash = df.apply(_row_hash, axis=1).reset_index(drop=True)
    out = out.reset_index(drop=True)
    out["row_hash"] = row_hash
    out["_bk"] = (
        out["invoice_no"].astype(str) + "|"
        + out["stock_code"].astype(str) + "|"
        + out["customer_id"].astype(str) + "|"
        + out["row_hash"]
    )
    return out


def build_quality_report(
    clean_counters: dict[str, int],
    fact: pd.DataFrame,
) -> dict[str, Any]:
    """Single dict summarising the run's data-quality outcome."""
    total = clean_counters["rows_in"] or 1
    pct = lambda n: round(100 * n / total, 2)
    unmatched_region = int((fact["region"] == "unknown").sum())
    return {
        "rows_input": clean_counters["rows_in"],
        "rows_output": clean_counters["rows_out"],
        "kept_pct": pct(clean_counters["rows_out"]),
        "dropped_missing_customer_id": clean_counters["dropped_missing_customer_id"],
        "dropped_missing_customer_id_pct": pct(clean_counters["dropped_missing_customer_id"]),
        "dropped_non_positive_quantity": clean_counters["dropped_non_positive_quantity"],
        "dropped_non_positive_price": clean_counters["dropped_non_positive_price"],
        "rows_with_unknown_region": unmatched_region,
        "unique_customers": int(fact["customer_id"].nunique()),
        "unique_invoices": int(fact["invoice_no"].nunique()),
        "unique_countries": int(fact["country"].nunique()),
        "revenue_eur_total": float(fact["revenue_eur"].sum().round(2)),
        "revenue_eur_min": float(fact["revenue_eur"].min().round(2)),
        "revenue_eur_max": float(fact["revenue_eur"].max().round(2)),
    }


# ------------------------------------------------------------------------------
# Airflow-facing entrypoint
# ------------------------------------------------------------------------------
@timed("transform")
def run_transform(extract_manifest: dict[str, Any]) -> dict[str, Any]:
    """Consume the extract manifest, produce a single Parquet to load."""
    cfg = load_config()

    sales = pd.read_parquet(extract_manifest["sales_parquet"])
    countries = pd.read_parquet(extract_manifest["countries_parquet"])
    fx_df = pd.read_parquet(extract_manifest["fx_parquet"])
    fx_rates = dict(zip(fx_df["date"], fx_df["fx_rate_gbp_eur"]))

    if sales.empty:
        log.info("nothing_to_transform")
        return {"records": 0, "fact_parquet": None, "quality_report": {}}

    sales, clean_counters = clean_sales(sales)
    sales = normalize_sales(sales)
    countries = normalize_countries(countries)
    enriched = enrich_with_countries(sales, countries)
    with_fx = apply_fx_historical(enriched, fx_rates)
    fact = build_fact_records(with_fx)

    report = build_quality_report(clean_counters, fact)
    log.info("data_quality_report", extra=report)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fact_path = cfg.staging_dir / f"fact_sales_{run_ts}.parquet"
    fact.to_parquet(fact_path, index=False)

    return {
        "records": int(len(fact)),
        "fact_parquet": str(fact_path),
        "quality_report": report,
    }
