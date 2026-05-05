"""Unit tests for the load stage.

We test the pure logic — operation building and the watermark contract —
by feeding small DataFrames in and asserting on the resulting
``UpdateOne`` ops. The MongoDB client itself is mocked, so these run
without a database.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pymongo import UpdateOne

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from etl import load  # noqa: E402


@pytest.fixture
def fact_batch() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "invoice_no": "A1", "stock_code": "X", "customer_id": "c1",
                "row_hash": "abc123",
                "quantity": 1, "unit_price_gbp": 10.0,
                "invoice_date": pd.Timestamp("2011-01-01", tz="UTC"),
                "country": "uk", "region": "Europe", "population": 1,
                "revenue_gbp": 10.0, "revenue_eur": 11.7, "fx_rate_gbp_eur": 1.17,
                "_bk": "A1|X|c1|abc123",
            },
            {
                "invoice_no": "A1", "stock_code": "X", "customer_id": "c1",
                "row_hash": "def456",  # same triple, different hash -> separate document
                "quantity": 2, "unit_price_gbp": 10.0,
                "invoice_date": pd.Timestamp("2011-01-01", tz="UTC"),
                "country": "uk", "region": "Europe", "population": 1,
                "revenue_gbp": 20.0, "revenue_eur": 23.4, "fx_rate_gbp_eur": 1.17,
                "_bk": "A1|X|c1|def456",
            },
        ]
    )


# ------------------------------------------------------------------------------
# Operation building
# ------------------------------------------------------------------------------
def test_build_operations_uses_full_business_key(fact_batch: pd.DataFrame) -> None:
    ops = load._build_operations(fact_batch)
    assert len(ops) == 2
    assert all(isinstance(o, UpdateOne) for o in ops)
    # The filter spec must include row_hash so the two rows above target
    # different documents and don't overwrite each other.
    filt0 = ops[0]._filter
    assert set(filt0.keys()) == {"invoice_no", "stock_code", "customer_id", "row_hash"}


def test_build_operations_strips_internal_bk_field(fact_batch: pd.DataFrame) -> None:
    """`_bk` is internal scaffolding for tests; it should not be persisted."""
    ops = load._build_operations(fact_batch)
    for op in ops:
        update_doc = op._doc["$set"]
        assert "_bk" not in update_doc


def test_build_operations_marks_upsert_true(fact_batch: pd.DataFrame) -> None:
    ops = load._build_operations(fact_batch)
    for op in ops:
        assert op._upsert is True


# ------------------------------------------------------------------------------
# Watermark contract
# ------------------------------------------------------------------------------
def test_update_watermark_persists_utc(monkeypatch) -> None:
    """A naive datetime must be coerced to UTC before being stored."""
    fake_meta = MagicMock()
    fake_client = MagicMock()
    fake_client.__getitem__.return_value.__getitem__.return_value = fake_meta

    cfg = MagicMock(mongodb_database="db", metadata_collection="_meta")
    naive = datetime(2011, 12, 9, 12, 50)
    load._update_watermark(fake_client, cfg, naive)

    args, _ = fake_meta.update_one.call_args
    stored_doc = args[1]["$set"]
    assert stored_doc["invoice_date"].tzinfo is not None
    assert stored_doc["invoice_date"].tzinfo == timezone.utc


def test_update_watermark_preserves_existing_tz() -> None:
    fake_meta = MagicMock()
    fake_client = MagicMock()
    fake_client.__getitem__.return_value.__getitem__.return_value = fake_meta
    cfg = MagicMock(mongodb_database="db", metadata_collection="_meta")

    aware = datetime(2011, 12, 9, 12, 50, tzinfo=timezone.utc)
    load._update_watermark(fake_client, cfg, aware)

    args, _ = fake_meta.update_one.call_args
    assert args[1]["$set"]["invoice_date"] == aware
