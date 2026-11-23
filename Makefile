.DEFAULT_GOAL := help

.PHONY: help setup hooks lint format format-check test check clean \
	up down ps logs storage-init reset psql-shop psql-platform catalog-init hosts-check bootstrap test-integration day dbt-deps dbt-debug dbt-build dbt-docs dbt-evaluate elementary-report warehouse-shell lint-sql fix-sql \
	airflow-up airflow-down airflow-check airflow-backfill airflow-day metrics-validate metrics-query marts-publish evidence-install evidence-dev evidence-validate iceberg-snapshots iceberg-files iceberg-partition iceberg-time-travel iceberg-fragment iceberg-compact iceberg-expire \
	ci-days ci-replay ci-verify-incremental ci-metrics ci-state ci-mode ci-slim ci-logs

-include .env
export

COMPOSE := docker compose -f infra/docker-compose.yml --env-file .env

# The simulated clock: the end of the latest arrival day `make day` has loaded. elementary's
# anomaly tests measure "now" against it instead of the wall clock (see dbt_project.yml).
SIM_CLOCK ?= data/simulated_now
DBT = SIMULATED_NOW=$$(cat $(SIM_CLOCK) 2>/dev/null) uv run dbt
DBT_ARGS := --project-dir dbt --profiles-dir dbt

# Evidence (ADR 0018): the CLI pinned by evidence/install_cli.sh, without telemetry. It reads the
# marts in Postgres as analytics_reader; older .env files lack the password, hence the default.
ANALYTICS_READER_PASSWORD ?= analytics-reader-dev
EVIDENCE_BIN ?= $(HOME)/.local/bin/evidence
EVIDENCE = cd evidence && DO_NOT_TRACK=1 EVIDENCE_TELEMETRY_DISABLED=1 $(EVIDENCE_BIN)

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Install dependencies from uv.lock and git hooks
	uv sync --frozen
	$(MAKE) hooks

hooks: ## Install pre-commit git hooks
	uv run pre-commit install

lint: lint-sql ## Lint Python with ruff and SQL with sqlfluff
	uv run ruff check .

# The dbt templater needs the package macros (dbt_utils, ...) but no stack: see the `lint` target
# in dbt/profiles.yml. Packages are installed only when missing or when their pins change.
dbt/dbt_packages: dbt/packages.yml dbt/package-lock.yml
	$(DBT) deps $(DBT_ARGS)
	@touch $@

lint-sql: dbt/dbt_packages ## Lint the dbt SQL with sqlfluff (no stack needed)
	uv run sqlfluff lint dbt/models dbt/tests

fix-sql: dbt/dbt_packages ## Auto-fix what sqlfluff can in the dbt SQL (review the diff)
	uv run sqlfluff fix dbt/models dbt/tests

format: ## Format with ruff
	uv run ruff format .

format-check: ## Check formatting without changing files
	uv run ruff format --check .

test: ## Run unit tests (no stack needed)
	uv run pytest -m "not integration"

test-integration: ## Run smoke tests against the running stack
	uv run pytest -m integration

check: lint format-check test dbt-evaluate ## Run the stackless CI checks (lint, format, unit tests, dbt audit)

clean: ## Remove caches
	rm -rf .ruff_cache .pytest_cache
	find . -name __pycache__ -type d -not -path "./.venv/*" -prune -exec rm -rf {} +

.env:
	cp .env.example .env

up: .env ## Start the local stack and wait until it is healthy
	$(COMPOSE) up -d --wait

down: ## Stop the stack, Airflow included (data volumes are kept)
	$(COMPOSE) --profile airflow down

ps: ## Show service status
	$(COMPOSE) --profile airflow ps

logs: ## Tail logs (SERVICE=name for one service)
	$(COMPOSE) logs -f --tail=100 $(SERVICE)

storage-init: .env ## Initialise Garage: layout, bucket, access key (idempotent)
	./infra/garage/init.sh

hosts-check: ## Verify the S3 hostname resolves on the host (needed once, see ADR 0012)
	@getent hosts garage >/dev/null 2>&1 || dscacheutil -q host -a name garage | grep -q 127.0.0.1 || \
		{ echo 'Host "garage" does not resolve. Run once: echo "127.0.0.1 garage" | sudo tee -a /etc/hosts'; exit 1; }
	@echo 'OK: "garage" resolves to 127.0.0.1'

bootstrap: hosts-check up storage-init catalog-init ## Bring up the whole stack from scratch (idempotent)

catalog-init: .env ## Bootstrap Lakekeeper and create the warehouse (idempotent)
	uv run python infra/lakekeeper/init_catalog.py

psql-shop: .env ## Open psql in the shop database (orders, CDC source)
	$(COMPOSE) exec postgres-shop sh -c 'psql -U "$$POSTGRES_USER" -d shop'

psql-platform: .env ## Open psql in the platform instance (Lakekeeper catalog)
	$(COMPOSE) exec postgres-platform sh -c 'psql -U "$$POSTGRES_USER" -d lakekeeper'

SEED ?= 42
comma := ,

day: .env ## Generate one day and load it through write-audit-publish: make day DATE=2026-01-10 [SEED=42] [FAULT=truncated|duplicated|bad_event_type]
	@test -n "$(DATE)" || { echo "usage: make day DATE=YYYY-MM-DD [SEED=42] [FAULT=...]"; exit 1; }
	uv run python -m generator --date $(DATE) --seed $(SEED) $(if $(FAULT),--fault $(FAULT))
	uv run python -m ingest --date $(DATE)
	@python3 ci/advance_clock.py $(DATE) $(SIM_CLOCK)

# --- Iceberg operations on bronze (ADR 0016) ---------------------------------------------------
ICEBERG := uv run python -m ingest.maintenance

iceberg-snapshots: .env ## Iceberg: main's snapshots (oldest first) and the refs of bronze
	$(ICEBERG) snapshots

iceberg-files: .env ## Iceberg: live data files of bronze per arrival day and partition spec
	$(ICEBERG) files

iceberg-partition: .env ## Iceberg: evolve the partitioning of bronze, metadata only: make iceberg-partition GRAIN=hour|day
	@test -n "$(GRAIN)" || { echo "usage: make iceberg-partition GRAIN=hour|day"; exit 1; }
	$(ICEBERG) partition --grain $(GRAIN)

iceberg-time-travel: .env ## Iceberg: a day's events before and after later days brought its late ones: make iceberg-time-travel DAY=2026-02-05
	@test -n "$(DAY)" || { echo "usage: make iceberg-time-travel DAY=YYYY-MM-DD"; exit 1; }
	$(ICEBERG) time-travel --day $(DAY)

iceberg-fragment: .env ## Iceberg: rewrite a day as many small files, like a streaming sink (demo for compaction): make iceberg-fragment DAY=... [PARTS=24]
	@test -n "$(DAY)" || { echo "usage: make iceberg-fragment DAY=YYYY-MM-DD [PARTS=24]"; exit 1; }
	$(ICEBERG) fragment --day $(DAY) --parts $(or $(PARTS),24)

iceberg-compact: .env ## Iceberg: merge the files of fragmented days (or DAY=... only)
	$(ICEBERG) compact $(if $(DAY),--day $(DAY))

iceberg-expire: .env ## Iceberg: expire all but the newest KEEP snapshots of main (default 5); ends time travel to them
	$(ICEBERG) expire --keep $(or $(KEEP),5)

# --- Airflow (ADR 0017) ----------------------------------------------------------------------
# A compose profile, so `make up`, the tests and CI run without it.
AIRFLOW = $(COMPOSE) --profile airflow
AIRFLOW_CLI = $(AIRFLOW) exec airflow-scheduler airflow

airflow-up: .env dbt/dbt_packages ## Airflow: build the image and start it on the running stack (UI on http://localhost:8088)
	$(AIRFLOW) up -d --build --wait

airflow-down: ## Airflow: stop its services, keep the rest of the stack
	$(AIRFLOW) stop airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer

airflow-check: ## Airflow: fail if a DAG does not import
	@$(AIRFLOW_CLI) dags list-import-errors -o plain | tee /dev/stderr | grep -q "No data found"

airflow-backfill: ## Airflow: replay arrival days FROM..TO, one after another: make airflow-backfill FROM=2026-02-08 TO=2026-02-10
	@test -n "$(FROM)" -a -n "$(TO)" || { echo "usage: make airflow-backfill FROM=YYYY-MM-DD TO=YYYY-MM-DD"; exit 1; }
	@# A paused DAG queues backfill runs without starting them; scheduled runs are skipped by the DAG.
	$(AIRFLOW_CLI) dags unpause --yes clickstream_daily
	$(AIRFLOW_CLI) backfill create --dag-id clickstream_daily --from-date $(FROM) --to-date $(TO) --max-active-runs 1 --reprocess-behavior failed

airflow-day: ## Airflow: run one arrival day by hand: make airflow-day DAY=2026-02-11 [FAULT=truncated|duplicated|bad_event_type] (a retry redelivers it fixed)
	@test -n "$(DAY)" || { echo "usage: make airflow-day DAY=YYYY-MM-DD [FAULT=...]"; exit 1; }
	$(AIRFLOW_CLI) dags unpause --yes clickstream_daily
	$(AIRFLOW_CLI) dags trigger clickstream_daily --logical-date $(DAY)T00:00:00+00:00 \
		--conf '{"day": "$(DAY)"$(if $(FAULT),$(comma) "fault": "$(FAULT)")}'

dbt-deps: ## Install the dbt packages (versions pinned in dbt/packages.yml)
	$(DBT) deps $(DBT_ARGS)

dbt-debug: .env ## Check the dbt connection: Iceberg catalog, Postgres, packages
	@mkdir -p data
	$(DBT) debug $(DBT_ARGS)

dbt-build: .env ## Run and test all dbt models (ARGS="--select ..." to narrow)
	@mkdir -p data
	$(DBT) build $(DBT_ARGS) $(ARGS)

dbt-docs: .env ## Build and serve the dbt docs with the lineage graph (http://localhost:8080)
	@mkdir -p data
	$(DBT) docs generate $(DBT_ARGS)
	$(DBT) docs serve $(DBT_ARGS) --port 8080

# The evaluator reads only the project graph, so it runs on the stackless `lint` target and its
# ~50 audit tables live in memory instead of the warehouse.
dbt-evaluate: dbt/dbt_packages ## Audit the dbt project (structure, tests, docs) with dbt_project_evaluator (no stack needed)
	$(DBT) build $(DBT_ARGS) --target lint --select package:dbt_project_evaluator dbt_project_evaluator_exceptions --vars '{enable_project_evaluator: true}'

# MetricFlow (ADR 0023) reads the semantic manifest that dbt parse writes, and queries the warehouse
# through the dev profile (from dbt/, so the warehouse path must be absolute).
MF = cd dbt && DBT_PROFILES_DIR=. DBT_DUCKDB_PATH=$(abspath $(or $(DBT_DUCKDB_PATH),data/warehouse.duckdb)) uv run mf

metrics-validate: .env ## Semantic layer: parse dbt, then check semantic models and metrics against the warehouse
	uv run dbt parse $(DBT_ARGS) --quiet
	$(MF) validate-configs

metrics-query: .env ## Semantic layer: make metrics-query METRICS=sessions,checkout_rate [GROUP_BY=metric_time__week,session__device]
	@test -n "$(METRICS)" || (echo "usage: make metrics-query METRICS=a,b [GROUP_BY=metric_time__day]"; exit 1)
	$(MF) query --metrics $(METRICS) --group-by $(or $(GROUP_BY),metric_time__day) --order $(firstword $(subst $(comma), ,$(or $(GROUP_BY),metric_time__day)))

marts-publish: .env ## Copy the marts from the DuckDB warehouse to Postgres analytics.marts, which Evidence reads
	SIMULATED_NOW=$$(cat $(SIM_CLOCK) 2>/dev/null) uv run python -m serving

evidence-install: ## Install the pinned Evidence CLI (checksum-verified) into ~/.local/bin
	evidence/install_cli.sh

evidence-validate: ## Check the Evidence pages, metrics and their queries against the published marts
	$(EVIDENCE) validate

evidence-dev: ## Serve the Evidence dashboard locally (run make marts-publish first)
	$(EVIDENCE) dev

elementary-report: ## Build the elementary report (tests, anomalies, run history) into data/elementary_report.html
	@# edr runs dbt from its own project directory, so the warehouse path must be absolute.
	DBT_DUCKDB_PATH=$(abspath $(or $(DBT_DUCKDB_PATH),data/warehouse.duckdb)) \
		uv run edr report --project-dir dbt --profiles-dir dbt \
		--file-path $(CURDIR)/data/elementary_report.html --open-browser false

warehouse-shell: ## Read-only DuckDB shell on the marts, in UTC (needs the duckdb CLI; no dbt run at the same time)
	duckdb -readonly data/warehouse.duckdb -cmd "SET TimeZone='UTC'"

# --- CI (ADR 0015) ---------------------------------------------------------------------------
# Everything CI does with the stack, runnable locally. The CI warehouse is data/ci/warehouse.duckdb
# (its file stem `warehouse` is the database name the slim build defers to), so a local run never
# touches data/warehouse.duckdb. Run it on an empty stack: the replay is only meaningful if bronze
# grows day by day, e.g. COMPOSE_PROJECT_NAME=clickstream-ci with the dev stack stopped.

# Four arrival days around the day utm_campaign appears (2026-02-01), so the replay crosses a
# schema change and late events from earlier days.
CI_DATES ?= 2026-01-31 2026-02-01 2026-02-02 2026-02-03
CI_DIR := data/ci
CI_STATE ?= $(CI_DIR)/state
CI_BASE ?= origin/main
CI_WAREHOUSE := DBT_DUCKDB_PATH=$(CI_DIR)/warehouse.duckdb
# The CI clock lives next to the CI warehouse, so a local CI run does not move the dev clock.
ci-%: SIM_CLOCK = $(CI_DIR)/simulated_now

ci-days: .env ## CI: generate and load the CI days without building dbt
	@for d in $(CI_DATES); do $(MAKE) --no-print-directory day DATE=$$d SEED=$(SEED) SIM_CLOCK=$(SIM_CLOCK) || exit 1; done

# Unit tests run once at the end: those of an incremental model mock `this`, and dbt reads the
# fixture's columns from the existing table, which an empty warehouse does not have yet.
ci-replay: .env dbt/dbt_packages ## CI: from an empty warehouse, load each CI day and dbt build after it
	rm -rf $(CI_DIR) && mkdir -p $(CI_DIR)
	@for d in $(CI_DATES); do \
		$(MAKE) --no-print-directory day DATE=$$d SEED=$(SEED) SIM_CLOCK=$(SIM_CLOCK) && \
		$(CI_WAREHOUSE) $(DBT) build $(DBT_ARGS) --exclude-resource-type unit_test || exit 1; \
	done
	$(CI_WAREHOUSE) $(DBT) test $(DBT_ARGS) --select test_type:unit

ci-verify-incremental: .env ## CI: after ci-replay, fail unless a full refresh of the incremental models changes nothing
	cp $(CI_DIR)/warehouse.duckdb $(CI_DIR)/incremental.duckdb
	$(CI_WAREHOUSE) $(DBT) run $(DBT_ARGS) --full-refresh --select config.materialized:incremental+
	uv run python ci/compare_warehouses.py $(CI_DIR)/incremental.duckdb $(CI_DIR)/warehouse.duckdb

ci-metrics: .env ## CI: after ci-replay, check the semantic layer against the CI warehouse and Evidence's metrics against it
	$(MAKE) --no-print-directory metrics-validate DBT_DUCKDB_PATH=$(CI_DIR)/warehouse.duckdb
	DBT_DUCKDB_PATH=$(CI_DIR)/warehouse.duckdb uv run pytest -q tests/integration/test_metric_definitions.py

ci-state: ## CI: collect the production state (manifest and warehouse) that pull requests defer to
	rm -rf $(CI_STATE) && mkdir -p $(CI_STATE)
	cp dbt/target/manifest.json $(CI_DIR)/warehouse.duckdb $(CI_STATE)/

ci-mode: .env dbt/dbt_packages ## CI: decide whether a pull request gets the slim build or the full replay
	./ci/choose_mode.sh $(abspath $(CI_STATE)) $(CI_BASE)

# elementary's own tables are built too: its run hooks and anomaly tests write to them in the PR
# database, which starts empty. They are small and hold only this run's results.
ci-slim: .env dbt/dbt_packages ## CI: build state:modified+ only, deferring the rest to the production state
	mkdir -p $(CI_DIR) && rm -f $(CI_DIR)/pr.duckdb
	DBT_DEFER_DUCKDB_PATH=$(CI_STATE)/warehouse.duckdb $(DBT) build $(DBT_ARGS) --target ci \
		--select state:modified+ package:elementary --defer --state $(abspath $(CI_STATE))

ci-logs: .env ## CI: print the last lines of every service log (no follow, for failed runs)
	$(COMPOSE) logs --no-color --tail=200

reset: .env ## Stop the stack and DELETE all data volumes
	$(COMPOSE) --profile airflow down -v
