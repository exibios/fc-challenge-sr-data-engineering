"""
Phase B — Orchestrated Batch Ingestion Layer
Phase C — Reverse ETL & Delivery Layer

One DAG, run daily, covering both phases end to end:

  extract_and_optimize -> validate_dq -> load_production -> reconcile
        -> aggregate_and_format -> deliver -> health_check_alerts

Every task is wrapped in `@audited("batch_reverse_etl")` from the shared
`common.audit` module — the exact same decorator the stream consumer uses
— so both the real-time and batch pipelines report into one `pipeline_runs`
table through one mechanism. Tasks stay plain functions: ingest data in,
return a dict with whatever `records_in/processed/rejected` applies, the
decorator handles persistence and failure logging on its own.
"""
import csv
import io
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow/common")
from db import get_connection                              # noqa: E402
from audit import audited, raise_alert, log_reconciliation  # noqa: E402

PARTNER_FILES_DIR = Path(os.environ.get("PARTNER_FILES_DIR", "/opt/airflow/data/partner_files"))
OPTIMIZED_DIR = Path(os.environ.get("OPTIMIZED_DIR", "/opt/airflow/data/optimized"))
DELIVERY_DIR = Path(os.environ.get("DELIVERY_DIR", "/opt/airflow/data/gdrive_shared_simulated"))
EXPECTED_COLUMNS = ["transaction_id", "account_id", "transaction_date", "amount", "reference_code"]
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y"]
REJECTION_RATE_ALERT_THRESHOLD = 0.10  # 10% of a run's rows rejected -> alert
STALE_FILE_ALERT_DAYS = 2


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="batch_reverse_etl",
    schedule="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args=default_args,
    tags=["challenge", "batch", "reverse-etl"],
)
def batch_reverse_etl():

    @task
    @audited("batch_reverse_etl")
    def extract_and_optimize() -> dict:
        """Read raw partner CSV/TXT files, tolerate ragged rows (extra/missing
        columns), and rewrite them as compressed Parquet. This is the 'local
        optimization' step: Parquet's columnar + snappy compression cuts file
        size (~5-10x vs raw CSV at this schema) and lets validate_dq scan
        only the columns it needs instead of re-parsing raw text every task.
        """
        OPTIMIZED_DIR.mkdir(parents=True, exist_ok=True)
        source_files = sorted(PARTNER_FILES_DIR.glob("partner_transactions_day_*.txt"))

        if not source_files:
            raise_alert(
                alert_type="MISSING_PARTNER_FILES",
                severity="CRITICAL",
                message=f"No partner files found in {PARTNER_FILES_DIR}",
            )
            raise FileNotFoundError(f"No partner files in {PARTNER_FILES_DIR}")

        rows = []
        for fpath in source_files:
            with open(fpath, newline="") as f:
                reader = csv.reader(f)
                next(reader)  # skip header
                for raw_fields in reader:
                    rows.append({
                        "source_file": fpath.name,
                        "field_count": len(raw_fields),
                        "raw_row": ",".join(raw_fields),
                        "transaction_id": raw_fields[0].strip() if len(raw_fields) > 0 else None,
                        "account_id": raw_fields[1].strip() if len(raw_fields) > 1 else None,
                        "transaction_date": raw_fields[2].strip() if len(raw_fields) > 2 else None,
                        "amount": raw_fields[3].strip() if len(raw_fields) > 3 else None,
                        "reference_code": raw_fields[4].strip() if len(raw_fields) > 4 else None,
                    })

        df = pd.DataFrame(rows)
        out_path = OPTIMIZED_DIR / "partner_transactions.parquet"
        df.to_parquet(out_path, compression="snappy", index=False)
        print(f"📦 Optimized {len(source_files)} files -> {out_path} ({len(df)} rows)")
        return {"records_in": len(df), "records_processed": len(df), "optimized_path": str(out_path)}

    @task
    @audited("batch_reverse_etl")
    def validate_dq(extract_result: dict) -> dict:
        """Data Quality Guardrails: null constraints, malformed schema
        (wrong column count), type-casting errors (amount/date), and
        in-batch duplicates. Valid/rejected rows are split and the rejected
        ones are persisted with a reason for later auditing — never silently
        dropped, and never allowed to crash the pipeline."""
        df = pd.read_parquet(extract_result["optimized_path"])

        valid_rows = []
        rejected_rows = []  # (source_file, raw_row, reason)
        seen_ids = set()

        for _, r in df.iterrows():
            reason = _validate_row(r, seen_ids)
            if reason:
                rejected_rows.append((r["source_file"], r["raw_row"], reason))
                continue
            seen_ids.add(r["transaction_id"])
            valid_rows.append({
                "transaction_id": r["transaction_id"],
                "account_id": r["account_id"],
                "transaction_date": _parse_date(r["transaction_date"]),
                "amount": _parse_amount(r["amount"]),
                "reference_code": r["reference_code"].strip(),
                "partner_code": _extract_partner_code(r["reference_code"]),
                "source_file": r["source_file"],
            })

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    """INSERT INTO rejected_transactions (source_file, raw_row, reason)
                       VALUES (%s, %s, %s)""",
                    rejected_rows,
                )
            conn.commit()
        finally:
            conn.close()

        valid_out = OPTIMIZED_DIR / "partner_transactions_valid.parquet"
        pd.DataFrame(valid_rows).to_parquet(valid_out, index=False)

        records_in = len(df)
        records_rejected = len(rejected_rows)
        if records_in > 0 and (records_rejected / records_in) >= REJECTION_RATE_ALERT_THRESHOLD:
            raise_alert(
                alert_type="HIGH_BATCH_REJECTION_RATE",
                severity="WARNING",
                message=f"{records_rejected}/{records_in} rows rejected in validate_dq",
                metric_value=records_rejected / records_in,
                threshold=REJECTION_RATE_ALERT_THRESHOLD,
            )
        print(f"✅ DQ: valid={len(valid_rows)} rejected={records_rejected}")
        return {
            "records_in": records_in, "records_processed": len(valid_rows), "records_rejected": records_rejected,
            "valid_path": str(valid_out), "source_row_count": records_in,
        }

    @task
    @audited("batch_reverse_etl")
    def load_production(dq_result: dict) -> dict:
        """Idempotent load: ON CONFLICT (transaction_id) DO NOTHING means
        re-running this DAG (or a partner resending a file) never double-counts
        a transaction in the production table."""
        df = pd.read_parquet(dq_result["valid_path"])

        conn = get_connection()
        inserted = 0
        try:
            with conn.cursor() as cur:
                for _, r in df.iterrows():
                    cur.execute(
                        """
                        INSERT INTO partner_transactions
                            (transaction_id, account_id, transaction_date, amount,
                             reference_code, partner_code, source_file)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (transaction_id) DO NOTHING
                        """,
                        (r["transaction_id"], r["account_id"], r["transaction_date"],
                         r["amount"], r["reference_code"], r["partner_code"], r["source_file"]),
                    )
                    inserted += cur.rowcount
            conn.commit()
        finally:
            conn.close()
        print(f"✅ Loaded {inserted} new rows into partner_transactions ({len(df) - inserted} were already present)")
        return {
            "records_in": len(df), "records_processed": inserted,
            "source_row_count": dq_result["source_row_count"], "loaded_count": inserted,
        }

    @task
    @audited("batch_reverse_etl")
    def reconcile(load_result: dict) -> dict:
        """Objective 2: verify what was captured at the source vs what
        successfully landed at the destination, logged over time so drift
        is visible across runs, not just within a single execution."""
        status = log_reconciliation(
            pipeline_name="batch_reverse_etl",
            run_date=datetime.utcnow().date(),
            source_count=load_result["source_row_count"],
            destination_count=load_result["loaded_count"],
        )
        print(f"🔎 Reconciliation status: {status} "
              f"(source={load_result['source_row_count']}, destination={load_result['loaded_count']})")
        # Variance here is expected — rejected + duplicate rows never land.
        # A real threshold-based check (e.g. "unexplained variance > rejected+dupe count")
        # would compare against validate_dq's rejected count; kept simple for this prototype.
        return {"status": status}

    @task
    @audited("batch_reverse_etl")
    def aggregate_and_format() -> dict:
        """Phase C: consolidated per-partner figures, formatted per the
        partner ingestion layout — a `row_count|N` metadata line injected
        above the standard CSV header."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT partner_code,
                           COUNT(*) AS transaction_count,
                           SUM(amount) AS total_amount
                    FROM partner_transactions
                    GROUP BY partner_code
                    ORDER BY partner_code
                    """
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        buf = io.StringIO()
        buf.write(f"row_count|{len(rows)}\n")
        writer = csv.writer(buf)
        writer.writerow(["partner_code", "transaction_count", "total_amount"])
        for r in rows:
            writer.writerow([r["partner_code"], r["transaction_count"], r["total_amount"]])

        OPTIMIZED_DIR.mkdir(parents=True, exist_ok=True)
        out_path = OPTIMIZED_DIR / f"partner_summary_{datetime.utcnow().strftime('%Y%m%d')}.csv"
        out_path.write_text(buf.getvalue())
        print(f"📝 Wrote summary report -> {out_path}")
        return {"records_processed": len(rows), "report_path": str(out_path)}

    @task
    @audited("batch_reverse_etl")
    def deliver(report_result: dict) -> dict:
        """Delivery Control: drop the final report into a local directory
        simulating a Google Drive shared folder."""
        DELIVERY_DIR.mkdir(parents=True, exist_ok=True)
        src = Path(report_result["report_path"])
        dest = DELIVERY_DIR / src.name
        dest.write_text(src.read_text())
        print(f"🚚 Delivered report -> {dest}")
        return {"records_processed": 1, "delivered_path": str(dest)}

    @task
    @audited("batch_reverse_etl")
    def health_check_alerts(_delivery_result: dict) -> dict:
        """Objective 3: proactive freshness/health check, independent of the
        execution tasks above — it only reads `pipeline_runs`/`alerts`
        history, so it can't be broken by (or couple monitoring to) the
        pipelines it's watching."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, ended_at FROM pipeline_runs
                    WHERE pipeline_name = 'batch_reverse_etl' AND task_name = 'load_production'
                    ORDER BY started_at DESC LIMIT 1
                    """
                )
                last_load = cur.fetchone()
        finally:
            conn.close()

        if last_load and last_load["ended_at"]:
            staleness_days = (datetime.utcnow() - last_load["ended_at"].replace(tzinfo=None)).days
            if staleness_days >= STALE_FILE_ALERT_DAYS:
                raise_alert(
                    alert_type="STALE_PARTNER_DATA",
                    severity="WARNING",
                    message=f"Last successful load was {staleness_days} day(s) ago",
                    metric_value=staleness_days,
                    threshold=STALE_FILE_ALERT_DAYS,
                )
        print("🩺 Health check complete.")
        return {}

    extract_result = extract_and_optimize()
    dq_result = validate_dq(extract_result)
    load_result = load_production(dq_result)
    reconcile_result = reconcile(load_result)
    report_result = aggregate_and_format()
    reconcile_result >> report_result
    delivery_result = deliver(report_result)
    health_check_alerts(delivery_result)


def _validate_row(r: pd.Series, seen_ids: set) -> str | None:
    if r["field_count"] != len(EXPECTED_COLUMNS):
        return "malformed_column_count"
    if not r["transaction_id"]:
        return "missing_transaction_id"
    if r["transaction_id"] in seen_ids:
        return "duplicate_transaction_id_in_batch"
    if not r["reference_code"] or not r["reference_code"].strip():
        return "missing_reference_code"
    if not re.match(r"^REF-[A-Z0-9_]+-\d+$", r["reference_code"].strip()):
        return "malformed_reference_code"
    try:
        amount = _parse_amount(r["amount"])
    except (TypeError, ValueError):
        return "invalid_amount_type"
    if amount <= 0:
        return "non_positive_amount"
    if _parse_date(r["transaction_date"]) is None:
        return "invalid_date_format"
    return None


def _parse_amount(raw: str) -> float:
    """Normalizes '$1400.00' -> 1400.00. Raises if truly non-numeric
    (e.g. 'CORRUPT_AMOUNT'), which validate_dq treats as a rejection."""
    cleaned = raw.replace("$", "").replace(",", "").strip()
    return float(cleaned)


def _parse_date(raw: str):
    if not raw:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _extract_partner_code(reference_code: str) -> str:
    match = re.match(r"^REF-([A-Z0-9_]+)-\d+$", reference_code.strip())
    return match.group(1) if match else "UNKNOWN"


batch_reverse_etl()
