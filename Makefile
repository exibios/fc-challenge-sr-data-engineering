.DEFAULT_GOAL := help
SHELL := /bin/bash

# ---------------------------------------------------------------------------
# Bankaya SR DE Challenge — project commands
# Run `make help` to see this list.
# ---------------------------------------------------------------------------

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

.PHONY: env
env: ## Create .env from .env.example if it doesn't exist yet
	@if [ -f .env ]; then \
		echo ".env already exists — leaving it alone."; \
	else \
		cp .env.example .env && echo "Created .env from .env.example — edit it if you need non-default credentials."; \
	fi

.PHONY: up
up: env ## Build images and start the full stack (Airflow, LocalStack, Postgres, stream consumer)
	docker compose up -d --build
	@echo ""
	@echo "Airflow UI:        http://localhost:8080  (see AIRFLOW_ADMIN_USER/PASSWORD in .env)"
	@echo "LocalStack Kinesis: http://localhost:4566"
	@echo "App Postgres:       localhost:5433 (see APP_DB_* in .env)"

.PHONY: down
down: ## Stop the stack (containers removed, volumes kept)
	docker compose down

.PHONY: destroy
destroy: ## Stop the stack AND wipe all volumes (full reset — you lose all DB/Airflow state)
	docker compose down -v

.PHONY: logs
logs: ## Tail logs from every service
	docker compose logs -f

.PHONY: logs-consumer
logs-consumer: ## Tail logs from the stream consumer only
	docker compose logs -f stream-consumer

.PHONY: logs-scheduler
logs-scheduler: ## Tail logs from the Airflow scheduler only
	docker compose logs -f airflow-scheduler

.PHONY: ps
ps: ## Show status of all services
	docker compose ps

.PHONY: generate
generate: ## Run the Kinesis event generator against the running stack (Ctrl+C to stop)
	python3 stream/stream_generator.py

.PHONY: trigger-dag
trigger-dag: ## Unpause and trigger the batch_reverse_etl DAG
	docker compose exec airflow-webserver airflow dags unpause batch_reverse_etl
	docker compose exec airflow-webserver airflow dags trigger batch_reverse_etl

.PHONY: dag-status
dag-status: ## Show recent runs of the batch DAG
	docker compose exec airflow-webserver airflow dags list-runs -d batch_reverse_etl

.PHONY: psql
psql: ## Open an interactive psql session against the app database
	docker compose exec app-postgres psql -U $${APP_DB_USER:-bankaya} -d $${APP_DB_NAME:-bankaya}

.PHONY: audit
audit: ## Quick look at the last 20 pipeline runs (governance Objective 1)
	docker compose exec app-postgres psql -U $${APP_DB_USER:-bankaya} -d $${APP_DB_NAME:-bankaya} \
		-c "SELECT pipeline_name, task_name, status, records_in, records_processed, records_rejected, started_at FROM pipeline_runs ORDER BY started_at DESC LIMIT 20;"

.PHONY: alerts
alerts: ## Quick look at the last 20 alerts raised (governance Objective 3)
	docker compose exec app-postgres psql -U $${APP_DB_USER:-bankaya} -d $${APP_DB_NAME:-bankaya} \
		-c "SELECT alert_type, severity, message, triggered_at FROM alerts ORDER BY triggered_at DESC LIMIT 20;"

.PHONY: rejected
rejected: ## Quick look at recently rejected/quarantined records (both phases)
	docker compose exec app-postgres psql -U $${APP_DB_USER:-bankaya} -d $${APP_DB_NAME:-bankaya} \
		-c "SELECT 'stream' AS phase, reason, rejected_at FROM rejected_stream_events \
		    UNION ALL \
		    SELECT 'batch' AS phase, reason, rejected_at FROM rejected_transactions \
		    ORDER BY rejected_at DESC LIMIT 20;"

.PHONY: reset-data
reset-data: ## Wipe locally-generated data (raw lake, optimized files, delivered reports) — keeps DB state
	rm -rf data/raw_lake/* data/optimized/* data/gdrive_shared_simulated/*
	@echo "Cleared generated data directories (data/partner_files/ untouched)."
