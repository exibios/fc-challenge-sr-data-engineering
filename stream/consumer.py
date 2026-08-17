"""
Phase A — Real-Time Ingestion Layer (Low Latency Sink)

Independent consumer for the `credit_applications_stream` Kinesis stream
(LocalStack). For every event it:

  1. Appends the RAW payload to the local data lake (JSONL, date-partitioned)
     — this happens *before* any validation, so the lake is a faithful,
     replayable copy of everything that arrived, corrupt or not.
  2. Validates + upserts into the operational store (Postgres) so the
     underwriting engine always reads clean, deduplicated data.
  3. Isolates malformed payloads into `rejected_stream_events` instead of
     crashing or silently dropping them.
  4. Emits audit telemetry per polling batch via `@audited(...)` — the same
     decorator the batch DAG uses (see common/audit.py).

Partitioning strategy for the raw lake: `dt=YYYY-MM-DD/events.jsonl`.
Justified in README — daily partitions keep files small enough to append
to cheaply, while giving downstream batch/replay jobs cheap date-range
pruning without needing an hourly-file explosion for this event volume.
"""
import base64
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "common"))

from db import get_connection            # noqa: E402
from audit import audited, raise_alert   # noqa: E402

STREAM_NAME = os.environ.get("KINESIS_STREAM_NAME", "credit_applications_stream")
ENDPOINT_URL = os.environ.get("KINESIS_ENDPOINT_URL", "http://localhost:4566")
LAKE_ROOT = Path(os.environ.get("LAKE_ROOT", "./data/raw_lake"))
POLL_INTERVAL_SECONDS = 5
REJECTION_RATE_ALERT_THRESHOLD = 0.15  # 15% of a polling batch rejected -> alert

REQUIRED_FIELDS = [
    "application_id", "customer_id", "requested_amount",
    "declared_income", "customer_age", "timestamp",
]


def get_kinesis_client():
    return boto3.client(
        "kinesis",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id=os.environ.get("KINESIS_ACCESS_KEY_ID", "mock"),
        aws_secret_access_key=os.environ.get("KINESIS_SECRET_ACCESS_KEY", "mock"),
    )


def wait_for_stream(client, retries=100):
    for _ in range(retries):
        try:
            client.describe_stream(StreamName=STREAM_NAME)
            return
        except client.exceptions.ResourceNotFoundException:
            print(f"⏳ Waiting for stream '{STREAM_NAME}' to be created by the generator...")
            time.sleep(5)
    raise RuntimeError(f"Stream '{STREAM_NAME}' never became available.")


def append_to_raw_lake(payload: dict):
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    partition_dir = LAKE_ROOT / f"dt={dt}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    with open(partition_dir / "events.jsonl", "a") as f:
        f.write(json.dumps(payload) + "\n")


def validate_payload(payload: dict):
    """Returns (is_valid, reason). Isolates Trap 1 & Trap 2 from stream_generator.py."""
    missing = [f for f in REQUIRED_FIELDS if payload.get(f) is None]
    if missing:
        return False, f"missing_required_fields:{','.join(missing)}"
    try:
        if float(payload["requested_amount"]) <= 0:
            return False, "non_positive_requested_amount"
        if float(payload["declared_income"]) <= 0:
            return False, "non_positive_declared_income"
    except (TypeError, ValueError):
        return False, "non_numeric_amount_field"
    return True, None


def quarantine_event(conn, payload: dict, reason: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO rejected_stream_events (application_id, raw_payload, reason)
            VALUES (%s, %s, %s)
            """,
            (payload.get("application_id"), json.dumps(payload), reason),
        )


def upsert_operational_store(conn, payload: dict) -> bool:
    """Idempotent write: ON CONFLICT DO NOTHING means a re-delivered /
    duplicated event (Trap 3) is a no-op instead of an error or a second row.
    Returns True if a new row was actually inserted (i.e. not a duplicate)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO credit_applications
                (application_id, customer_id, requested_amount, declared_income,
                 customer_age, event_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (application_id) DO NOTHING
            """,
            (
                payload["application_id"], payload["customer_id"],
                payload["requested_amount"], payload["declared_income"],
                payload["customer_age"], payload["timestamp"],
            ),
        )
        return cur.rowcount == 1


@audited("stream_ingestion", task_name="poll_batch")
def process_poll_batch(records) -> dict:
    """One audited unit of work per Kinesis poll. Returns the dict shape
    `@audited` reads (`records_in/processed/rejected`) — everything else
    is plumbing detail the decorator doesn't need to know about."""
    records_in = records_processed = records_rejected = 0

    if records:
        conn = get_connection()
        try:
            for r in records:
                raw_bytes = r["Data"]
                try:
                    payload = json.loads(raw_bytes)
                except json.JSONDecodeError:
                    append_to_raw_lake({"_unparseable": True, "raw": base64.b64encode(raw_bytes).decode()})
                    quarantine_event(conn, {}, "unparseable_json")
                    records_rejected += 1
                    continue

                append_to_raw_lake(payload)  # land raw BEFORE validation, always
                records_in += 1

                is_valid, reason = validate_payload(payload)
                if not is_valid:
                    quarantine_event(conn, payload, reason)
                    records_rejected += 1
                    continue

                upsert_operational_store(conn, payload)  # idempotent either way
                records_processed += 1
            conn.commit()
        finally:
            conn.close()

        if records_in > 0:
            rejection_rate = records_rejected / records_in
            if rejection_rate >= REJECTION_RATE_ALERT_THRESHOLD:
                raise_alert(
                    alert_type="HIGH_STREAM_REJECTION_RATE",
                    severity="CRITICAL",
                    message=f"{records_rejected}/{records_in} events rejected in this poll batch",
                    metric_value=rejection_rate,
                    threshold=REJECTION_RATE_ALERT_THRESHOLD,
                )
        print(f"✅ Batch: in={records_in} processed={records_processed} rejected={records_rejected}")

    return {"records_in": records_in, "records_processed": records_processed,
            "records_rejected": records_rejected}


def run_consumer():
    client = get_kinesis_client()
    wait_for_stream(client)

    stream_desc = client.describe_stream(StreamName=STREAM_NAME)
    shard_id = stream_desc["StreamDescription"]["Shards"][0]["ShardId"]
    shard_iterator = client.get_shard_iterator(
        StreamName=STREAM_NAME, ShardId=shard_id, ShardIteratorType="LATEST",
    )["ShardIterator"]

    print("🚀 Stream consumer started — polling for credit application events...")

    while True:
        resp = client.get_records(ShardIterator=shard_iterator, Limit=100)
        shard_iterator = resp["NextShardIterator"]
        records = resp.get("Records", [])
        if records:  # skip auditing empty polls — avoids flooding pipeline_runs
            process_poll_batch(records)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_consumer()
