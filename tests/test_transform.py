"""Unit tests for the transform stage.

These exercise the pure data-manipulation functions -- no Airflow,
no MongoDB, no network. Run with:

    docker compose run --rm airflow-scheduler bash -lc "cd /opt/airflow && pytest tests/ -v"
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from etl.transform import (  # noqa: E402
    COUNTRY_ALIASES,
    apply_fx_historical,
    build_fact_records,
    build_quality_report,
    clean_sales,
    enrich_with_countries,
    normalize_countries,
    normalize_sales,
)


# ------------------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------------------
@pytest.fixture
def raw_sales() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "InvoiceNo": ["A1", "A2", "A3", "A4", "A5"],
            "StockCode": ["X", "Y", "Z", "W", "V"],
            "Description": ["a", "b", "c", "d", "e"],
            "Quantity": [2, -1, 3, 5, 1],
            "UnitPrice": [10.0, 5.0, 0.0, 2.5, 4.0],
            "InvoiceDate": pd.to_datetime(
                ["2011-01-01", "2011-01-02", "2011-01-03", "2011-01-04", "2011-01-05"],
                utc=True,
            ),
            "CustomerID": ["c1", "c2", "c3", None, "  C5  "],
            "Country": ["United Kingdom", "France", "Germany", "Spain", "  United Kingdom  "],
        }
    )


@pytest.fixture
def countries() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "country": ["United Kingdom", "France", "Germany", "Spain"],
            "region": ["Europe", "Europe", "Europe", "Europe"],
            "population": [67000000, 65000000, 83000000, 47000000],
        }
    )


@pytest.fixture
def fx_rates() -> dict[str, float]:
    """Per-date GBP->EUR rates spanning the test fixture's invoice dates."""
    return {
        "2011-01-01": 1.17,
        "2011-01-02": 1.17,
        "2011-01-03": 1.18,
        "2011-01-04": 1.18,
        "2011-01-05": 1.19,
    }


# ------------------------------------------------------------------------------
# Cleaning
# ------------------------------------------------------------------------------
def test_clean_drops_bad_rows(raw_sales: pd.DataFrame) -> None:
    cleaned, counters = clean_sales(raw_sales)
    assert len(cleaned) == 2
    assert set(cleaned["InvoiceNo"]) == {"A1", "A5"}
    assert counters["dropped_missing_customer_id"] == 1
    assert counters["dropped_non_positive_quantity"] == 1
    assert counters["dropped_non_positive_price"] == 1


def test_clean_returns_counters_summing_to_input(raw_sales: pd.DataFrame) -> None:
    """Quality contract: every input row is either kept or accounted for in a drop bucket."""
    _, counters = clean_sales(raw_sales)
    accounted = (
        counters["rows_out"]
        + counters["dropped_missing_customer_id"]
        + counters["dropped_non_positive_quantity"]
        + counters["dropped_non_positive_price"]
    )
    assert accounted == counters["rows_in"]


# ------------------------------------------------------------------------------
# Country alias resolution (new tests -- May 2026)
# ------------------------------------------------------------------------------
def test_country_aliases_resolve_before_join() -> None:
    """EIRE, USA, RSA, Channel Islands and Czech Republic must be translated to
    their REST Countries canonical names so they receive a real region instead
    of falling back to 'unknown'."""
    alias_sales = pd.DataFrame(
        {
            "InvoiceNo":   ["A1",   "A2",  "A3",              "A4",    "A5"],
            "StockCode":   ["X",    "Y",   "Z",               "W",     "V"],
            "Description": ["a",    "b",   "c",               "d",     "e"],
            "Quantity":    [1,      1,     1,                 1,       1],
            "UnitPrice":   [10.0,   10.0,  10.0,              10.0,    10.0],
            "InvoiceDate": pd.to_datetime(["2011-01-01"] * 5, utc=True),
            "CustomerID":  ["c1",   "c2",  "c3",              "c4",    "c5"],
            "Country":     ["EIRE", "USA", "Channel Islands",  "RSA",   "Czech Republic"],
        }
    )
    normalized = normalize_sales(alias_sales)
    expected = {
        "A1": "ireland",
        "A2": "united states",
        "A3": "jersey",
        "A4": "south africa",
        "A5": "czechia",
    }
    for inv, expected_key in expected.items():
        row = normalized[normalized["InvoiceNo"] == inv]
        assert row["country_key"].iloc[0] == expected_key, (
            f"Invoice {inv}: expected country_key='{expected_key}', "
            f"got '{row['country_key'].iloc[0]}'"
        )


def test_alias_countries_get_real_region() -> None:
    """After alias resolution, formerly-unknown countries must receive a region
    from the API lookup rather than 'unknown'."""
    alias_sales = pd.DataFrame(
        {
            "InvoiceNo":   ["A1"],
            "StockCode":   ["X"],
            "Description": ["a"],
            "Quantity":    [1],
            "UnitPrice":   [10.0],
            "InvoiceDate": pd.to_datetime(["2011-01-01"], utc=True),
            "CustomerID":  ["c1"],
            "Country":     ["EIRE"],
        }
    )
    api_countries = pd.DataFrame(
        {
            "country":    ["Ireland"],
            "region":     ["Europe"],
            "population": [5_000_000],
        }
    )
    normalized = normalize_sales(alias_sales)
    enriched = enrich_with_countries(normalized, normalize_countries(api_countries))
    assert enriched["region"].iloc[0] == "Europe"
    assert enriched["population"].iloc[0] == 5_000_000


def test_unresolvable_countries_remain_unknown() -> None:
    """'Unspecified' and 'European Community' have no valid alias and must
    still fall back to region='unknown' -- not silently dropped."""
    no_match_sales = pd.DataFrame(
        {
            "InvoiceNo":   ["A1",          "A2"],
            "StockCode":   ["X",           "Y"],
            "Description": ["a",           "b"],
            "Quantity":    [1,             1],
            "UnitPrice":   [10.0,          10.0],
            "InvoiceDate": pd.to_datetime(["2011-01-01", "2011-01-01"], utc=True),
            "CustomerID":  ["c1",          "c2"],
            "Country":     ["Unspecified", "European Community"],
        }
    )
    normalized = normalize_sales(no_match_sales)
    api_countries = pd.DataFrame(
        {"country": ["France"], "region": ["Europe"], "population": [65_000_000]}
    )
    enriched = enrich_with_countries(normalized, normalize_countries(api_countries))
    assert len(enriched) == 2
    assert (enriched["region"] == "unknown").all()


# ------------------------------------------------------------------------------
# Normalization & enrichment
# ------------------------------------------------------------------------------
def test_normalize_trims_and_lowercases(raw_sales: pd.DataFrame) -> None:
    cleaned, _ = clean_sales(raw_sales)
    normalized = normalize_sales(cleaned)
    a5 = normalized[normalized["InvoiceNo"] == "A5"].iloc[0]
    assert a5["Country"] == "united kingdom"
    assert a5["CustomerID"] == "c5"


def test_enrichment_adds_region_and_population(raw_sales, countries) -> None:
    cleaned, _ = clean_sales(raw_sales)
    normalized = normalize_sales(cleaned)
    enriched = enrich_with_countries(normalized, normalize_countries(countries))
    assert "region" in enriched.columns
    assert (enriched["region"] == "Europe").all()


def test_enrichment_preserves_unmatched_rows() -> None:
    sales = pd.DataFrame(
        {
            "InvoiceNo": ["A1"], "StockCode": ["X"], "Description": ["a"],
            "Quantity": [1], "UnitPrice": [10.0],
            "InvoiceDate": pd.to_datetime(["2011-01-01"], utc=True),
            "CustomerID": ["c1"], "Country": ["Atlantis"],
        }
    )
    normalized = normalize_sales(sales)
    countries = normalize_countries(
        pd.DataFrame({"country": ["France"], "region": ["Europe"], "population": [65_000_000]})
    )
    enriched = enrich_with_countries(normalized, countries)
    assert len(enriched) == 1
    assert enriched["region"].iloc[0] == "unknown"
    assert enriched["population"].iloc[0] == 0


# ------------------------------------------------------------------------------
# FX (historical, per-date)
# ------------------------------------------------------------------------------
def test_fx_uses_per_date_rate(raw_sales, fx_rates) -> None:
    cleaned, _ = clean_sales(raw_sales)
    normalized = normalize_sales(cleaned).assign(region="Europe", population=1)
    with_fx = apply_fx_historical(normalized, fx_rates)
    a1 = with_fx[with_fx["InvoiceNo"] == "A1"].iloc[0]
    a5 = with_fx[with_fx["InvoiceNo"] == "A5"].iloc[0]
    assert a1["fx_rate_gbp_eur"] == pytest.approx(1.17)
    assert a5["fx_rate_gbp_eur"] == pytest.approx(1.19)
    assert a1["revenue_eur"] == pytest.approx(20.0 * 1.17)
    assert a5["revenue_eur"] == pytest.approx(4.0 * 1.19)


def test_fx_falls_back_to_previous_day_for_gaps() -> None:
    """Weekends/holidays missing from the FX table must fall forward, not break."""
    sales = pd.DataFrame(
        {
            "InvoiceNo": ["A1"], "StockCode": ["X"], "Description": ["a"],
            "Quantity": [1], "UnitPrice": [10.0],
            "InvoiceDate": pd.to_datetime(["2011-01-08"], utc=True),
            "CustomerID": ["c1"], "Country": ["uk"],
            "region": ["Europe"], "population": [1],
        }
    )
    rates = {"2011-01-07": 1.20}
    out = apply_fx_historical(sales, rates)
    assert out["fx_rate_gbp_eur"].iloc[0] == pytest.approx(1.20)


# ------------------------------------------------------------------------------
# Business key & idempotency
# ------------------------------------------------------------------------------
def test_business_key_includes_row_hash(raw_sales, countries, fx_rates) -> None:
    cleaned, _ = clean_sales(raw_sales)
    normalized = normalize_sales(cleaned)
    enriched = enrich_with_countries(normalized, normalize_countries(countries))
    fact = build_fact_records(apply_fx_historical(enriched, fx_rates))
    assert "row_hash" in fact.columns
    assert "_bk" in fact.columns
    assert fact["_bk"].is_unique


def test_business_key_disambiguates_genuine_duplicates() -> None:
    """Two rows with identical (invoice, stock, customer) but different qty
    must produce different business keys."""
    sales = pd.DataFrame(
        {
            "InvoiceNo": ["A1", "A1"],
            "StockCode": ["X", "X"],
            "Description": ["a", "a"],
            "Quantity": [1, 2],
            "UnitPrice": [10.0, 10.0],
            "InvoiceDate": pd.to_datetime(["2011-01-01", "2011-01-01"], utc=True),
            "CustomerID": ["c1", "c1"],
            "Country": ["uk", "uk"],
            "region": ["Europe", "Europe"],
            "population": [1, 1],
        }
    )
    fact = build_fact_records(apply_fx_historical(sales, {"2011-01-01": 1.17}))
    assert fact["_bk"].nunique() == 2


def test_pipeline_is_deterministic(raw_sales, countries, fx_rates) -> None:
    """Re-running with identical inputs must yield identical business keys."""
    def pipeline() -> list[str]:
        cleaned, _ = clean_sales(raw_sales)
        s = normalize_sales(cleaned)
        c = normalize_countries(countries)
        return list(build_fact_records(apply_fx_historical(enrich_with_countries(s, c), fx_rates))["_bk"])
    assert pipeline() == pipeline()


# ------------------------------------------------------------------------------
# Quality report
# ------------------------------------------------------------------------------
def test_quality_report_has_expected_keys(raw_sales, countries, fx_rates) -> None:
    cleaned, counters = clean_sales(raw_sales)
    s = normalize_sales(cleaned)
    fact = build_fact_records(
        apply_fx_historical(enrich_with_countries(s, normalize_countries(countries)), fx_rates)
    )
    report = build_quality_report(counters, fact)
    for key in ("rows_input", "rows_output", "kept_pct", "unique_customers", "revenue_eur_total"):
        assert key in report
    assert report["rows_input"] == 5
    assert report["rows_output"] == 2
