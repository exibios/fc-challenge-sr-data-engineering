# Bankaya SR Data Engineer Challenge — Reference Prototype

A local, reproducible prototype demonstrating engineering control over a
**low-latency streaming flow** (real-time credit underwriting) and a
**high-consistency batch flow** (daily partner reconciliation), unified
under one governance/observability layer.

> **Status: first draft.** Core logic (consumer, DAG, DQ, audit model) is
> implemented and the DQ rules have been validated against the 5 sample
> partner files. Not yet stood up end-to-end in Docker in this environment —
> see [Running it](#running-it) and [Known gaps](#known-gaps--next-steps).

## Architecture

```
                     ┌─────────────────────────────────────────┐
                     │            LocalStack (Kinesis)          │
                     │        credit_applications_stream        │
                     └───────────────────┬───────────────────────┘
                                          │
                                          ▼
                     ┌─────────────────────────────────────────┐
   stream_generator.py ──push──▶│         stream/consumer.py (Phase A)      │
   (injects dup/malformed)      │                                            │
                                 │  1. append RAW → raw_lake/dt=YYYY-MM-DD/  │
                                 │  2. validate → quarantine if bad          │
                                 │  3. idempotent UPSERT → operational store │
                                 └─────────────┬─────────────┬───────────────┘
                                               │             │
                                               ▼             ▼
                                  credit_applications   rejected_stream_events
                                     (Postgres)             (Postgres)

   ─────────────────────────────────────────────────────────────────────────

   data/partner_files/*.txt (Phase B)         Airflow DAG: batch_reverse_etl
        │                                     ┌──────────────────────────┐
        └──extract_and_optimize──▶ Parquet ──▶│  validate_dq             │
                                               │  load_production (idemp.)│
                                               │  reconcile                │
                                               │  aggregate_and_format     │  (Phase C)
                                               │  deliver → gdrive_sim/    │
                                               │  health_check_alerts      │
                                               └──────────────────────────┘
                                                          │
                                                          ▼
                        pipeline_runs · reconciliation_log · alerts
                            (shared governance tables — common/audit.py)
```

The stream consumer and the Airflow DAG never duplicate governance logic —
both import `common/audit.py` and `common/db.py`, so "when did it run / how
many records / what failed" is answered identically for both flows from one
`pipeline_runs` table.

### Governance implementation: why a decorator, not a framework

Two "proper" alternatives were evaluated before settling on a plain
`@audited(...)` decorator (`common/audit.py`):

- **Airflow's Listener API** (`airflow.listeners`) can auto-instrument every
  task in every DAG via a global plugin, with zero code inside the tasks
  themselves. Rejected because: it only covers Airflow tasks — the stream
  consumer isn't Airflow and would still need its own wrapper, so you end
  up explaining *two* instrumentation mechanisms instead of one; plugin
  discovery requires a scheduler restart to pick up changes; retries fire
  the listener again for the same logical run unless you key off
  `try_number`; and it uses Airflow's own metadata-DB session, which is a
  trap if you're not careful not to reuse it for app-DB writes.
- **OpenLineage** (`apache-airflow-providers-openlineage`) is the actual
  production-grade framework for this problem, and Airflow supports it as
  a drop-in provider. Rejected *for this prototype* only because its
  reference backend (Marquez) is another container + failure surface in a
  "one-click" stack — not because it's the wrong tool for a real
  deployment. It's the natural upgrade path if this went to production.

A decorator gets ~90% of the boilerplate reduction of either option with
zero added infrastructure, and — importantly for a live walkthrough —
stays visible in the function signature (`@audited("batch_reverse_etl")`
sitting right above `def validate_dq`) instead of firing from an implicit
plugin hook somewhere else in the codebase.

## Justification: raw lake partitioning

`raw_lake/dt=YYYY-MM-DD/events.jsonl`, one append-only JSONL file per day.

- **Scalability**: at this event volume (a few events/sec, bursty), an
  hourly or per-shard partition would produce a large number of tiny files —
  worse for both write throughput (constant file-open overhead) and later
  batch reads (small-file problem on any downstream engine). Daily keeps
  file counts low while still giving cheap date-range pruning.
- **Query efficiency**: `dt=` is a Hive-style partition key, so any
  downstream engine (Spark, Athena, BigQuery external tables, dbt on
  DuckDB) can prune directly on folder name instead of scanning file
  contents.
- **Replayability**: every raw event is landed *before* validation, so the
  lake is always a complete, corruption-tolerant record — if a DQ rule
  turns out to be wrong, we can reprocess from the lake without re-pulling
  from Kinesis.
- If volume grew materially, the natural next partition level is
  `dt=.../hour=...`, not a change in format.

## Justification: operational store choice (Postgres)

Chosen over Redis/DynamoDB/MongoDB for this use case specifically:

- **Access pattern**: the underwriting engine's read is a point lookup by
  `application_id` — not a fan-out or range query. A relational PK lookup
  on an indexed column is single-digit-millisecond and doesn't need a
  purpose-built KV store.
- **Idempotence for free**: `INSERT ... ON CONFLICT (application_id) DO
  NOTHING` gives exactly the dedupe behavior Phase A asks for, with no
  extra application-level locking or conditional-write logic (which Mongo/
  DynamoDB would both require to achieve the same guarantee).
- **One fewer moving part** in a "one-click" docker-compose prototype: no
  separate cache-invalidation or cache-warm story to design and explain.
- **Trade-off, stated plainly**: if underwriting read volume grew into the
  tens of thousands of req/s with sub-millisecond SLA, Redis in front of
  Postgres (cache-aside) would be the next step — not a replacement.

## Governance & observability model

| Objective | Mechanism |
|---|---|
| 1. Auditability | `pipeline_runs` table: `run_id, pipeline_name, task_name, started_at, ended_at, records_in, records_processed, records_rejected, status, error_message, error_trace`. Written via the `@audited(...)` decorator — wraps a function, catches exceptions, records full trace, **re-raises** (auditing never masks a failure). |
| 2. Graceful degradation & reconciliation | Bad records are isolated, never dropped: `rejected_stream_events` (Phase A) / `rejected_transactions` (Phase B), each with a `reason`. `reconcile` task compares source row counts vs. rows actually landed and logs to `reconciliation_log` every run, so drift is visible over time, not just per-run. |
| 3. Proactive health & alerts | `raise_alert(...)` writes structured rows to `alerts` (type, severity, metric, threshold) — decoupled from execution logic (stream consumer and DAG both call it, neither owns it). Triggers: rejection-rate threshold breaches, missing partner files, stale last-successful-load. In production this table would be tailed by Prometheus/Grafana or piped to Slack/PagerDuty — deliberately **not** wired to a real alerting SaaS here, see trade-off note below. |

**Why not New Relic/Datadog for this prototype**: the deliverable is a
one-click, fully local docker-compose stack — a SaaS APM tool breaks that
(external account, API key, egress) and isn't built for the row-level DQ
telemetry these objectives actually ask for (records rejected, reconciled
counts, quarantine reasons). A generic APM tool is the right *addition* in
a real production deployment, layered on top of this same
`pipeline_runs`/`alerts` model — not a replacement for it.

## Data quality rules (validated against the 5 sample files)

| File | Anomaly | Rule triggered |
|---|---|---|
| day_1 | `CORRUPT_AMOUNT` | `invalid_amount_type` |
| day_2 | empty `transaction_id` | `missing_transaction_id` |
| day_2 | exact duplicate row | `duplicate_transaction_id_in_batch` |
| day_3 | `16/05/2026` (DD/MM/YYYY) | normalized via fallback format, **not rejected** |
| day_3 | `-120.00` | `non_positive_amount` |
| day_4 | extra trailing column | `malformed_column_count` |
| day_4 | empty `reference_code` | `missing_reference_code` |
| day_5 | `$1400.00`, trailing whitespace | normalized (`$`/whitespace stripped), **not rejected** |

25 rows in, 18 valid, 7 rejected — confirmed by running the validation logic
standalone against the uploaded files before wiring it into the DAG.

The `16/05/2026` and `$1400.00` cases are deliberately treated as
**cleanable**, not rejectable — real partner feeds have format drift, and a
DQ layer that rejects everything it can safely normalize just pushes the
cleanup problem onto whoever reviews the quarantine table. Truly ambiguous
or unparseable values (`CORRUPT_AMOUNT`, missing required fields, wrong
column counts) are rejected and quarantined with a reason.

## Repo layout

```
docker-compose.yml          # one-click infra: Airflow, LocalStack, Postgres
Makefile                    # `make help` for every project command
.env.example                # config/credentials template — copy to .env
.gitignore                  # excludes .env, generated data, runtime noise
init-db/01_init.sql         # schema: operational store, production tables,
                             #   quarantine tables, governance tables
common/
  db.py                     # shared Postgres connection helper (reads env vars)
  audit.py                  # @audited decorator / raise_alert / log_reconciliation
stream/
  consumer.py               # Phase A: Kinesis consumer
  stream_generator.py       # provided script (unmodified)
  Dockerfile
dags/
  batch_reverse_etl_dag.py  # Phase B + Phase C, one DAG
data/
  partner_files/            # provided sample CSVs (tracked in git)
  raw_lake/                 # written by the stream consumer (gitignored)
  optimized/                # Parquet + final report, written by the DAG (gitignored)
  gdrive_shared_simulated/  # "delivery" target for Phase C (gitignored)
```

## Configuration

All credentials and config live in `.env` (gitignored) — `.env.example` is
the committed template with safe local-dev defaults. `docker-compose.yml`
loads it via `env_file:` for every service, and `common/db.py` reads the
same variable names via `os.environ[...]`, so there's exactly one place
that defines a password or connection string.

```bash
cp .env.example .env   # or just `make up`, which does this automatically
```

## Running it

```bash
make up              # copies .env.example -> .env if missing, builds, starts everything
make generate         # starts the Kinesis event generator (Ctrl+C to stop)
make trigger-dag       # unpauses + triggers the batch_reverse_etl DAG

make logs             # tail everything
make audit            # last 20 pipeline_runs rows (Objective 1)
make alerts            # last 20 alerts raised (Objective 3)
make rejected          # recently quarantined records, both phases (Objective 2)
make psql             # interactive psql session against the app DB

make down             # stop, keep volumes
make destroy           # stop AND wipe all volumes — full reset
make help             # see every command
```

Airflow UI: `http://localhost:8080` (credentials in `.env`, default `admin`/`admin`)
LocalStack Kinesis: `http://localhost:4566`
App Postgres (host access): `localhost:5433` (credentials in `.env`, default `bankaya`/`bankaya`)

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `stream-consumer` loops on "Waiting for stream..." | Generator hasn't created the Kinesis stream yet | Start `stream_generator.py`; it calls `create_stream` on boot. |
| DAG task fails at `get_connection()` | `app-postgres` not healthy yet / wrong host from inside vs. outside compose | Inside containers use `app-postgres:5432`; from host use `localhost:5433`. |
| `_PIP_ADDITIONAL_REQUIREMENTS` slows container startup | Expected — packages install on every boot | Fine for this prototype; swap for a custom Airflow image (`Dockerfile` + `requirements.txt`) if iterating a lot. |
| No rows in `partner_transactions` after a DAG run | Check `rejected_transactions` and `alerts` first | The pipeline never crashes on bad data — it quarantines it. Run `make rejected`; a 0-row load usually means every source row hit a DQ rule. |
| Want to see governance data directly | `make psql`, `make audit`, or `make alerts` | e.g. `SELECT * FROM pipeline_runs ORDER BY started_at DESC;` |
| `docker compose` complains about missing env vars | `.env` doesn't exist yet | `make up` creates it automatically, or run `make env` / `cp .env.example .env` directly. |

## Known gaps / next steps

- Not yet run end-to-end inside Docker in this environment (network-
  sandboxed while drafting) — DQ logic was validated standalone against the
  real sample files; full `docker compose up` should be the first thing run
  to shake out any container-wiring issues.
- `reconcile` task compares source-row-count vs. loaded-count in aggregate;
  a stronger version would reconcile per source file / per partner and
  assert `source == loaded + rejected` exactly, flagging anything that
  doesn't add up (not just "counts differ").
- No Prometheus/Grafana wired up yet — `alerts` table is ready to be
  tailed by either; noted as the natural next step in a real deployment.
- Presentation/demo deck not yet built.
