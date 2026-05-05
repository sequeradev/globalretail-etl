"""Unit tests for the extract stage.

We test the parts that are pure logic (chunked CSV reading + watermark
filtering, API response shape validation, FX-rate sanity bounds) by
mocking the network and filesystem boundaries. No real CSV, no real
HTTP, no MongoDB.
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from etl import extract  # noqa: E402
from etl.config import Config  # noqa: E402


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _cfg(tmp_path: Path, csv: Path | None = None, chunksize: int = 2) -> Config:
    return Config(
        csv_path=csv or tmp_path / "missing.csv",
        countries_api_url="http://fake/countries",
        frankfurter_api_url="http://fake/fx",
        base_currency="GBP",
        target_currency="EUR",
        staging_dir=tmp_path / "staging",
        mongodb_uri="mongodb://fake",
        mongodb_database="db",
        mongodb_collection="c",
        metadata_collection="_meta",
        batch_size=10,
        api_timeout_seconds=1,
        api_max_retries=1,
        csv_chunksize=chunksize,
        staging_retention_days=7,
    )


def _write_csv(path: Path, rows: list[tuple]) -> None:
    pd.DataFrame(
        rows,
        columns=[
            "InvoiceNo", "StockCode", "Description", "Quantity",
            "UnitPrice", "InvoiceDate", "CustomerID", "Country",
        ],
    ).to_csv(path, index=False, encoding="ISO-8859-1")


# ------------------------------------------------------------------------------
# CSV reading + watermark
# ------------------------------------------------------------------------------
def test_read_csv_full_load_returns_all_rows(tmp_path: Path) -> None:
    csv = tmp_path / "sales.csv"
    _write_csv(csv, [
        ("A1", "X", "d", 1, 10.0, "2011-01-01", "c1", "UK"),
        ("A2", "Y", "d", 2, 20.0, "2011-01-02", "c2", "FR"),
        ("A3", "Z", "d", 3, 30.0, "2011-01-03", "c3", "DE"),
    ])
    df = extract.read_sales_csv(_cfg(tmp_path, csv=csv), watermark=None)
    assert len(df) == 3


def test_read_csv_watermark_filters_old_rows(tmp_path: Path) -> None:
    csv = tmp_path / "sales.csv"
    _write_csv(csv, [
        ("A1", "X", "d", 1, 10.0, "2011-01-01", "c1", "UK"),
        ("A2", "Y", "d", 2, 20.0, "2011-01-05", "c2", "FR"),
        ("A3", "Z", "d", 3, 30.0, "2011-01-10", "c3", "DE"),
    ])
    wm = datetime(2011, 1, 4, tzinfo=timezone.utc)
    df = extract.read_sales_csv(_cfg(tmp_path, csv=csv), watermark=wm)
    assert len(df) == 2
    assert set(df["InvoiceNo"]) == {"A2", "A3"}


def test_read_csv_chunked_reads_dont_lose_rows(tmp_path: Path) -> None:
    """Splitting the read into chunks of 2 must still produce the same 5 rows."""
    csv = tmp_path / "sales.csv"
    _write_csv(csv, [
        (f"A{i}", "X", "d", 1, 10.0, f"2011-01-0{i}", f"c{i}", "UK")
        for i in range(1, 6)
    ])
    df = extract.read_sales_csv(_cfg(tmp_path, csv=csv, chunksize=2), watermark=None)
    assert len(df) == 5


def test_read_csv_raises_when_file_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        extract.read_sales_csv(_cfg(tmp_path), watermark=None)


# ------------------------------------------------------------------------------
# REST Countries — schema validation
# ------------------------------------------------------------------------------
def _mock_response(json_body) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = json_body
    resp.raise_for_status.return_value = None
    return resp


def test_fetch_countries_happy_path(tmp_path: Path) -> None:
    body = [
        {"name": {"common": f"Country{i}"}, "region": "Europe", "population": 1000}
        for i in range(150)
    ]
    with patch.object(extract.requests, "get", return_value=_mock_response(body)):
        df = extract.fetch_countries(_cfg(tmp_path))
    assert len(df) == 150
    assert {"country", "region", "population"}.issubset(df.columns)


def test_fetch_countries_rejects_too_few_rows(tmp_path: Path) -> None:
    """If the API returns suspiciously few countries, refuse rather than
    silently degrade enrichment for downstream steps."""
    body = [{"name": {"common": "Solo"}, "region": "X", "population": 1}]
    with patch.object(extract.requests, "get", return_value=_mock_response(body)):
        with pytest.raises(ValueError, match="expected at least"):
            extract.fetch_countries(_cfg(tmp_path))


def test_fetch_countries_rejects_non_list_payload(tmp_path: Path) -> None:
    with patch.object(extract.requests, "get", return_value=_mock_response({"oops": True})):
        with pytest.raises(ValueError, match="non-list"):
            extract.fetch_countries(_cfg(tmp_path))


# ------------------------------------------------------------------------------
# Frankfurter — historical FX + sanity bounds
# ------------------------------------------------------------------------------
def test_fetch_fx_for_date_returns_rate(tmp_path: Path) -> None:
    body = {"rates": {"EUR": 1.17}}
    with patch.object(extract.requests, "get", return_value=_mock_response(body)):
        rate = extract.fetch_exchange_rate_for_date(_cfg(tmp_path), date(2011, 1, 5))
    assert rate == pytest.approx(1.17)


def test_fetch_fx_rejects_out_of_band_rate(tmp_path: Path) -> None:
    """A rate of 9.99 GBP->EUR is non-physical and must fail loudly."""
    body = {"rates": {"EUR": 9.99}}
    with patch.object(extract.requests, "get", return_value=_mock_response(body)):
        with pytest.raises(ValueError, match="sanity bounds"):
            extract.fetch_exchange_rate_for_date(_cfg(tmp_path), date(2011, 1, 5))


def test_fetch_fx_rates_for_dates_calls_per_date(tmp_path: Path) -> None:
    body = {"rates": {"EUR": 1.17}}
    with patch.object(extract.requests, "get", return_value=_mock_response(body)) as m:
        rates = extract.fetch_fx_rates_for_dates(
            _cfg(tmp_path),
            [date(2011, 1, 1), date(2011, 1, 2), date(2011, 1, 3)],
        )
    assert len(rates) == 3
    assert m.call_count == 3
