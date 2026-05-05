"""Structured logging utilities.

- `get_logger(name)` returns a stdlib logger configured to emit one-line
  JSON records. This makes logs trivially parseable by observability
  backends (Loki, ELK, Datadog, CloudWatch) without changing application code.

- `@timed(task_name)` decorator logs start/end/elapsed for a function and
  re-raises any exception after logging it. This is the primary way the
  pipeline satisfies the PDF's "record the start, number of records
  processed, and total time taken for each task" requirement.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from functools import wraps
from typing import Any, Callable


class JsonFormatter(logging.Formatter):
    """Formatter that emits records as single-line JSON."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Attach any extra=... fields the caller passed in.
        reserved = {
            "args", "asctime", "created", "exc_info", "exc_text", "filename",
            "funcName", "levelname", "levelno", "lineno", "message", "module",
            "msecs", "msg", "name", "pathname", "process", "processName",
            "relativeCreated", "stack_info", "thread", "threadName", "taskName",
        }
        for key, value in record.__dict__.items():
            if key not in reserved and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger. Idempotent — calling twice won't duplicate handlers."""
    logger = logging.getLogger(name)
    if getattr(logger, "_configured", False):
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False  # don't double-log through Airflow's root logger
    logger._configured = True  # type: ignore[attr-defined]
    return logger


def timed(task_name: str) -> Callable:
    """Decorator that logs task start, elapsed seconds, and record counts.

    Convention: if the wrapped function returns a dict containing a
    ``records`` key, that count is logged too.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            log = get_logger(f"etl.{task_name}")
            start = time.perf_counter()
            log.info("task_start", extra={"task": task_name})
            try:
                result = func(*args, **kwargs)
            except Exception:
                elapsed = time.perf_counter() - start
                log.exception(
                    "task_failed",
                    extra={"task": task_name, "elapsed_sec": round(elapsed, 3)},
                )
                raise
            elapsed = time.perf_counter() - start
            extra: dict[str, Any] = {"task": task_name, "elapsed_sec": round(elapsed, 3)}
            if isinstance(result, dict) and "records" in result:
                extra["records"] = result["records"]
            log.info("task_done", extra=extra)
            return result

        return wrapper

    return decorator
