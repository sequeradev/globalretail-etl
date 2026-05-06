"""Centralized configuration.

All tunables live here. No hard-coded paths or URLs scattered across
the codebase — this makes it trivial to test and to promote between
environments (local / staging / prod).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    # --- Sources ---
    csv_path: Path
    countries_api_url: str
    frankfurter_api_url: str
    base_currency: str  # source currency in the CSV
    target_currency: str  # currency we report revenue in

    # --- Staging ---
    staging_dir: Path

    # --- MongoDB ---
    mongodb_uri: str
    mongodb_database: str
    mongodb_collection: str
    metadata_collection: str

    # --- Pipeline behaviour ---
    batch_size: int
    api_timeout_seconds: int
    api_max_retries: int
    csv_chunksize: int          # rows per CSV read chunk; bounds peak memory
    staging_retention_days: int  # how many days of staging Parquet files to keep


def load_config() -> Config:
    """Build a Config from environment variables, failing fast on missing ones."""
    mongodb_uri = os.environ.get("MONGODB_URI")
    if not mongodb_uri:
        raise RuntimeError(
            "MONGODB_URI environment variable is required. "
            "Check your .env file and docker-compose.yml."
        )

    return Config(
        csv_path=Path(os.environ.get("CSV_PATH", "/opt/airflow/include/data/online_retail.csv")),
        countries_api_url="https://restcountries.com/v3.1/all?fields=name,region,population",
        frankfurter_api_url="https://api.frankfurter.dev/v1",
        base_currency="GBP",
        target_currency="EUR",
        staging_dir=Path(os.environ.get("STAGING_DIR", "/opt/airflow/include/staging")),
        mongodb_uri=mongodb_uri,
        mongodb_database=os.environ.get("MONGODB_DATABASE", "globalretail"),
        mongodb_collection=os.environ.get("MONGODB_COLLECTION", "fact_sales"),
        metadata_collection="_pipeline_metadata",
        batch_size=int(os.environ.get("BATCH_SIZE", "1000")),
        api_timeout_seconds=int(os.environ.get("API_TIMEOUT", "30")),
        api_max_retries=int(os.environ.get("API_MAX_RETRIES", "5")),
        csv_chunksize=int(os.environ.get("CSV_CHUNKSIZE", "100000")),
        staging_retention_days=int(os.environ.get("STAGING_RETENTION_DAYS", "7")),
    )
