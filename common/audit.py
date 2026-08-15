"""Shared observability primitives (Governance Objectives 1-3).

Both the stream consumer and every Airflow task use the SAME `@audited(...)`
decorator, so the telemetry model is identical across the low-latency and
batch pipelines — one mechanism, one file, no plugin registration, no
Airflow-specific machinery to explain.

Design note (see README "Governance implementation" section for the full
trade-off writeup): Airflow ships a Listener API (`airflow.listeners`) that
can auto-instrument every task via a global plugin, and OpenLineage
(`apache-airflow-providers-openlineage`) is the production-grade framework
for this exact problem. Both were evaluated and deliberately not used here:
the Listener API only covers Airflow tasks (the stream consumer still needs
its own wrapper either way, so you'd end up explaining two mechanisms
instead of one), and OpenLineage's reference backend (Marquez) is an extra
container + failure surface that isn't worth it for a local take-home. A
plain decorator gets 90% of the boilerplate reduction with zero added
infrastructure and stays visible in the function signature — easier to
defend line-by-line in a walkthrough than an implicit plugin hook.
"""
import functools
import traceback
import uuid
from datetime import datetime, timezone

from db import get_connection


def _persist(run_id, pipeline_name, task_name, started_at, ended_at,
             records_in, records_processed, records_rejected,
             status, error_message=None, error_trace=None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs
                    (run_id, pipeline_name, task_name, started_at, ended_at,
                     records_in, records_processed, records_rejected,
                     status, error_message, error_trace)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    ended_at = EXCLUDED.ended_at,
                    records_in = EXCLUDED.records_in,
                    records_processed = EXCLUDED.records_processed,
                    records_rejected = EXCLUDED.records_rejected,
                    status = EXCLUDED.status,
                    error_message = EXCLUDED.error_message,
                    error_trace = EXCLUDED.error_trace
                """,
                (
                    str(run_id), pipeline_name, task_name, started_at, ended_at,
                    records_in, records_processed, records_rejected,
                    status, error_message, error_trace,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def audited(pipeline_name: str, task_name: str = None):
    """Decorator: wraps a function as one auditable pipeline run.

    The wrapped function should return a dict. If it contains any of
    `records_in` / `records_processed` / `records_rejected`, those are
    captured into `pipeline_runs` automatically — everything else in the
    dict passes through untouched to the caller (e.g. Airflow XCom).

    Usage:
        @audited("batch_reverse_etl")
        def validate_dq(optimized_path: str) -> dict:
            ...
            return {"records_in": n, "records_processed": p,
                    "records_rejected": r, "valid_path": str(out)}

    Failures are logged with full stack trace and RE-RAISED — auditing
    must never mask a failure (Airflow still needs to see the task fail).
    """
    def decorator(fn):
        name = task_name or fn.__name__

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            run_id = uuid.uuid4()
            started_at = datetime.now(timezone.utc)
            _persist(run_id, pipeline_name, name, started_at, started_at,
                     0, 0, 0, "RUNNING")
            try:
                result = fn(*args, **kwargs)
                result = result or {}
                _persist(
                    run_id, pipeline_name, name, started_at, datetime.now(timezone.utc),
                    result.get("records_in", 0),
                    result.get("records_processed", 0),
                    result.get("records_rejected", 0),
                    "SUCCESS",
                )
                return result
            except Exception as e:
                _persist(
                    run_id, pipeline_name, name, started_at, datetime.now(timezone.utc),
                    0, 0, 0, "FAILED",
                    error_message=str(e), error_trace=traceback.format_exc(),
                )
                raise
        return wrapper
    return decorator


def raise_alert(alert_type: str, severity: str, message: str,
                 metric_value: float = None, threshold: float = None):
    """Objective 3: mock alert dispatch, decoupled from execution logic.

    Kept as a plain function (not part of the decorator) — thresholds and
    alerting conditions are business logic that belongs inside the task,
    not something a generic instrumentation layer should infer.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alerts (alert_type, severity, message, metric_value, threshold)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (alert_type, severity, message, metric_value, threshold),
            )
        conn.commit()
        print(f"🚨 [{severity}] {alert_type}: {message}")
    finally:
        conn.close()


def log_reconciliation(pipeline_name: str, run_date, source_count: int, destination_count: int):
    """Objective 2: source-vs-destination reconciliation, logged over time."""
    status = "MATCHED" if source_count == destination_count else "VARIANCE_DETECTED"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO reconciliation_log
                    (pipeline_name, run_date, source_count, destination_count, status)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (pipeline_name, run_date, source_count, destination_count, status),
            )
        conn.commit()
    finally:
        conn.close()
    if status == "VARIANCE_DETECTED":
        raise_alert(
            alert_type="RECONCILIATION_VARIANCE",
            severity="WARNING",
            message=f"{pipeline_name}: source={source_count} vs destination={destination_count} on {run_date}",
            metric_value=abs(source_count - destination_count),
            threshold=0,
        )
    return status
