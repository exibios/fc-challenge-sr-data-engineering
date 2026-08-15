-- =========================================================================
-- Bankaya SR DE Challenge — Application Database Schema
-- One DB, four concerns: operational store, production (batch) tables,
-- quarantine tables, and governance/observability tables.
-- =========================================================================

-- -------------------------------------------------------------------------
-- OPERATIONAL STORE (Phase A) — read path for the underwriting engine.
-- Postgres chosen here: single-digit-ms point lookups by application_id
-- are all the underwriting engine needs (no fan-out queries), and we get
-- idempotent upserts via ON CONFLICT for free instead of hand-rolling
-- dedupe logic that Redis/Mongo would require. See README for trade-offs.
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS credit_applications (
    application_id      TEXT PRIMARY KEY,
    customer_id          TEXT NOT NULL,
    requested_amount      NUMERIC(14, 2) NOT NULL,
    declared_income        NUMERIC(14, 2) NOT NULL,
    customer_age          INTEGER NOT NULL,
    event_timestamp        TIMESTAMPTZ NOT NULL,
    ingested_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rejected_stream_events (
    id                   BIGSERIAL PRIMARY KEY,
    application_id        TEXT,
    raw_payload           JSONB NOT NULL,
    reason               TEXT NOT NULL,
    rejected_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------------------
-- PRODUCTION TABLES (Phase B) — daily partner transactions, post-DQ.
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS partner_transactions (
    transaction_id        TEXT PRIMARY KEY,
    account_id            TEXT NOT NULL,
    transaction_date       DATE NOT NULL,
    amount               NUMERIC(14, 2) NOT NULL,
    reference_code         TEXT NOT NULL,
    partner_code          TEXT NOT NULL,
    source_file           TEXT NOT NULL,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rejected_transactions (
    id                   BIGSERIAL PRIMARY KEY,
    source_file           TEXT NOT NULL,
    raw_row              TEXT NOT NULL,
    reason               TEXT NOT NULL,
    rejected_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------------------
-- GOVERNANCE / OBSERVABILITY (shared by stream consumer + Airflow DAG)
-- -------------------------------------------------------------------------

-- Objective 1: End-to-end pipeline auditability
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id               UUID PRIMARY KEY,
    pipeline_name          TEXT NOT NULL,
    task_name             TEXT,
    started_at            TIMESTAMPTZ NOT NULL,
    ended_at              TIMESTAMPTZ,
    records_in            INTEGER DEFAULT 0,
    records_processed      INTEGER DEFAULT 0,
    records_rejected       INTEGER DEFAULT 0,
    status               TEXT NOT NULL DEFAULT 'RUNNING'
                            CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    error_message          TEXT,
    error_trace            TEXT
);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name_time
    ON pipeline_runs (pipeline_name, started_at DESC);

-- Objective 2: Reconciliation — source counts vs landed counts over time
CREATE TABLE IF NOT EXISTS reconciliation_log (
    id                   BIGSERIAL PRIMARY KEY,
    pipeline_name          TEXT NOT NULL,
    run_date              DATE NOT NULL,
    source_count           INTEGER NOT NULL,
    destination_count       INTEGER NOT NULL,
    variance              INTEGER GENERATED ALWAYS AS (source_count - destination_count) STORED,
    status               TEXT NOT NULL CHECK (status IN ('MATCHED', 'VARIANCE_DETECTED')),
    checked_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Objective 3: Proactive health checks / alert dispatch (mocked)
CREATE TABLE IF NOT EXISTS alerts (
    id                   BIGSERIAL PRIMARY KEY,
    alert_type            TEXT NOT NULL,
    severity              TEXT NOT NULL CHECK (severity IN ('WARNING', 'CRITICAL')),
    message               TEXT NOT NULL,
    metric_value           NUMERIC,
    threshold             NUMERIC,
    triggered_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
