"""Load stage.

Strategy
--------
* A **unique compound index** on (invoice_no, stock_code, customer_id, row_hash)
  is created on first run. The row_hash component prevents legitimate duplicates
  in the source (same invoice + product + customer recorded as separate
  adjustment lines) from silently overwriting each other on upsert.

* We use ``bulk_write`` with ``UpdateOne(..., upsert=True)`` in batches of 1000
  (the PDF's explicit requirement). This is truly idempotent — re-running the
  DAG for the same date range updates the same documents in place instead of
  inserting copies.

* After a successful load, we atomically update the watermark document
  ``_pipeline_metadata/last_watermark`` to the max invoice_date just loaded.
  Doing this *last* means that if the load fails halfway the next run will
  reprocess the same window — losing no records.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd
from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

from etl.config import Config, load_config
from etl.logger import get_logger, timed

log = get_logger("etl.load")


def _ensure_indexes(client: MongoClient, cfg: Config) -> None:
    """Create business-key unique index + an InvoiceDate index for query speed."""
    coll = client[cfg.mongodb_database][cfg.mongodb_collection]
    coll.create_index(
        [
            ("invoice_no", ASCENDING),
            ("stock_code", ASCENDING),
            ("customer_id", ASCENDING),
            ("row_hash", ASCENDING),
        ],
        unique=True,
        name="uq_business_key",
    )
    coll.create_index([("invoice_date", ASCENDING)], name="ix_invoice_date")
    coll.create_index([("region", ASCENDING)], name="ix_region")


def _build_operations(batch: pd.DataFrame) -> list[UpdateOne]:
    """Turn a dataframe slice into a list of upsert ops keyed by the full business key."""
    ops: list[UpdateOne] = []
    for rec in batch.to_dict(orient="records"):
        filt = {
            "invoice_no": rec["invoice_no"],
            "stock_code": rec["stock_code"],
            "customer_id": rec["customer_id"],
            "row_hash": rec["row_hash"],
        }
        rec.pop("_bk", None)
        ops.append(UpdateOne(filt, {"$set": rec}, upsert=True))
    return ops


def _update_watermark(client: MongoClient, cfg: Config, new_watermark: datetime) -> None:
    """Persist the new high-water mark. Always store in UTC."""
    if new_watermark.tzinfo is None:
        new_watermark = new_watermark.replace(tzinfo=timezone.utc)
    meta = client[cfg.mongodb_database][cfg.metadata_collection]
    meta.update_one(
        {"_id": "last_watermark"},
        {
            "$set": {
                "invoice_date": new_watermark,
                "updated_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )
    log.info("watermark_updated", extra={"watermark": new_watermark.isoformat()})


@timed("load")
def run_load(transform_manifest: dict[str, Any]) -> dict[str, Any]:
    """Bulk-upsert the transformed records and advance the watermark."""
    cfg = load_config()
    fact_path = transform_manifest.get("fact_parquet")

    if not fact_path:
        log.info("nothing_to_load")
        return {"records": 0, "upserted": 0, "modified": 0}

    df = pd.read_parquet(fact_path)
    if df.empty:
        log.info("empty_fact_parquet")
        return {"records": 0, "upserted": 0, "modified": 0}

    # MongoDB's BSON date handles tz-aware datetimes fine; pymongo will
    # convert pandas Timestamps automatically.

    total_upserted = 0
    total_modified = 0

    with MongoClient(cfg.mongodb_uri, serverSelectionTimeoutMS=10_000) as client:
        _ensure_indexes(client, cfg)
        coll = client[cfg.mongodb_database][cfg.mongodb_collection]

        for start in range(0, len(df), cfg.batch_size):
            batch = df.iloc[start : start + cfg.batch_size]
            ops = _build_operations(batch)
            try:
                result = coll.bulk_write(ops, ordered=False)
            except BulkWriteError as bwe:
                # Surface the actual write errors so they're not swallowed
                log.error(
                    "bulk_write_error",
                    extra={
                        "batch_start": start,
                        "write_errors": bwe.details.get("writeErrors", [])[:3],
                    },
                )
                raise

            total_upserted += result.upserted_count
            total_modified += result.modified_count
            log.info(
                "batch_loaded",
                extra={
                    "batch_start": start,
                    "batch_size": len(batch),
                    "upserted": result.upserted_count,
                    "modified": result.modified_count,
                },
            )

        # Advance watermark to the maximum invoice_date we just loaded.
        max_date = pd.to_datetime(df["invoice_date"]).max().to_pydatetime()
        _update_watermark(client, cfg, max_date)

    return {
        "records": int(len(df)),
        "upserted": int(total_upserted),
        "modified": int(total_modified),
    }
