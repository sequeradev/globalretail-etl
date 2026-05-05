"""GlobalRetail ETL — main DAG.

Flow:
    extract_data >> transform_data >> load_to_mongo >> cleanup_staging

Design choices worth calling out:

* **TaskFlow API (@task / @dag)** — less boilerplate than the classic
  PythonOperator, and XCom is handled implicitly by return value.

* **Manifests through XCom, data through a shared volume** — each task
  returns a small dict (counts + paths) and writes its heavy output as
  Parquet into ``/opt/airflow/include/staging``. Shoving 500k rows through
  XCom would pickle them into the metadata DB, which is the #1 way people
  blow up Airflow in production.

* **Task retries** — 2 retries with exponential backoff at the task level,
  on top of tenacity retries inside each API call. The two layers
  protect against different failure modes (transient HTTP flake vs.
  container OOM / scheduler hiccup).

* **on_failure_callback** — emits a structured JSON alert when a task
  exhausts its retries. In production this is wired to an alerting backend
  (PagerDuty, Slack); here it lands in stdout for the scheduler logs to
  pick up so failed runs are not silent.

* **cleanup_staging** — a final task that prunes Parquet files older than
  ``staging_retention_days``. Without it the staging directory would grow
  unbounded across daily runs.

* **catchup=False** — the DAG is idempotent thanks to the watermark, so
  there's no benefit to running backfills as separate logical dates.

* **Schedule** — daily at 02:00 UTC. Override via Airflow UI if needed.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task

from etl.config import load_config
from etl.extract import run_extract
from etl.transform import run_transform
from etl.load import run_load
from etl.logger import get_logger


_alert_log = get_logger("etl.alerts")


def alert_on_failure(context: dict[str, Any]) -> None:
    """Emit a structured failure record. Wire to PagerDuty/Slack in production.

    Lives at module scope so Airflow can pickle it into worker processes.
    """
    ti = context.get("task_instance")
    _alert_log.error(
        "task_failed_alert",
        extra={
            "dag_id": context.get("dag").dag_id if context.get("dag") else None,
            "task_id": ti.task_id if ti else None,
            "run_id": context.get("run_id"),
            "try_number": ti.try_number if ti else None,
            "exception": str(context.get("exception")),
            "log_url": ti.log_url if ti else None,
        },
    )


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "email_on_failure": False,        # SMTP-based alerts disabled; we use callback instead
    "email_on_retry": False,
    "on_failure_callback": alert_on_failure,
}


@dag(
    dag_id="globalretail_etl",
    description="Incremental ETL: Online Retail CSV + REST Countries + FX -> MongoDB Atlas",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "ecommerce", "capstone"],
)
def globalretail_etl():

    @task(task_id="extract_data")
    def extract_data() -> dict[str, Any]:
        return run_extract()

    @task(task_id="transform_data")
    def transform_data(extract_manifest: dict[str, Any]) -> dict[str, Any]:
        return run_transform(extract_manifest)

    @task(task_id="load_to_mongo")
    def load_to_mongo(transform_manifest: dict[str, Any]) -> dict[str, Any]:
        return run_load(transform_manifest)

    @task(task_id="cleanup_staging", trigger_rule="all_done")
    def cleanup_staging() -> dict[str, int]:
        """Delete staging Parquet files older than the retention window.

        ``trigger_rule='all_done'`` means cleanup runs even when an upstream
        task fails — leftover files from failed runs would otherwise pile up
        forever. Failed-run files are still useful for debugging until they
        age out, so retention is measured from file mtime, not from success.
        """
        cfg = load_config()
        cutoff = time.time() - cfg.staging_retention_days * 86400
        deleted = 0
        kept = 0
        for f in Path(cfg.staging_dir).glob("*.parquet"):
            if f.stat().st_mtime < cutoff:
                f.unlink(missing_ok=True)
                deleted += 1
            else:
                kept += 1
        _alert_log.info(
            "staging_cleanup",
            extra={"deleted": deleted, "kept": kept, "retention_days": cfg.staging_retention_days},
        )
        return {"deleted": deleted, "kept": kept}

    extract_manifest = extract_data()
    transform_manifest = transform_data(extract_manifest)
    load_result = load_to_mongo(transform_manifest)
    load_result >> cleanup_staging()


dag = globalretail_etl()
