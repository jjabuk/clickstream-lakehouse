.DEFAULT_GOAL := help

.PHONY: help setup hooks lint format format-check test check clean \
	up down ps logs storage-init reset psql-shop psql-platform catalog-init hosts-check bootstrap test-integration day dbt-deps dbt-debug dbt-build dbt-docs dbt-evaluate warehouse-shell lint-sql fix-sql \
	ci-days ci-replay ci-verify-incremental ci-state ci-mode ci-slim ci-logs

-include .env
export

COMPOSE := docker compose -f infra/docker-compose.yml --env-file .env
DBT := uv run dbt
DBT_ARGS := --project-dir dbt --profiles-dir dbt

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

down: ## Stop the stack (data volumes are kept)
	$(COMPOSE) down

ps: ## Show service status
	$(COMPOSE) ps

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

day: .env ## Generate one day and load it: make day DATE=2026-01-10 [SEED=42]
	@test -n "$(DATE)" || { echo "usage: make day DATE=YYYY-MM-DD [SEED=42]"; exit 1; }
	uv run python -m generator --date $(DATE) --seed $(SEED)
	uv run python -m ingest --date $(DATE)

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

ci-days: .env ## CI: generate and load the CI days without building dbt
	@for d in $(CI_DATES); do $(MAKE) --no-print-directory day DATE=$$d SEED=$(SEED) || exit 1; done

# Unit tests run once at the end: those of an incremental model mock `this`, and dbt reads the
# fixture's columns from the existing table, which an empty warehouse does not have yet.
ci-replay: .env dbt/dbt_packages ## CI: from an empty warehouse, load each CI day and dbt build after it
	rm -rf $(CI_DIR) && mkdir -p $(CI_DIR)
	@for d in $(CI_DATES); do \
		$(MAKE) --no-print-directory day DATE=$$d SEED=$(SEED) && \
		$(CI_WAREHOUSE) $(DBT) build $(DBT_ARGS) --exclude-resource-type unit_test || exit 1; \
	done
	$(CI_WAREHOUSE) $(DBT) test $(DBT_ARGS) --select test_type:unit

ci-verify-incremental: .env ## CI: after ci-replay, fail unless a full refresh of the incremental models changes nothing
	cp $(CI_DIR)/warehouse.duckdb $(CI_DIR)/incremental.duckdb
	$(CI_WAREHOUSE) $(DBT) run $(DBT_ARGS) --full-refresh --select config.materialized:incremental+
	uv run python ci/compare_warehouses.py $(CI_DIR)/incremental.duckdb $(CI_DIR)/warehouse.duckdb

ci-state: ## CI: collect the production state (manifest and warehouse) that pull requests defer to
	rm -rf $(CI_STATE) && mkdir -p $(CI_STATE)
	cp dbt/target/manifest.json $(CI_DIR)/warehouse.duckdb $(CI_STATE)/

ci-mode: .env dbt/dbt_packages ## CI: decide whether a pull request gets the slim build or the full replay
	./ci/choose_mode.sh $(abspath $(CI_STATE)) $(CI_BASE)

ci-slim: .env dbt/dbt_packages ## CI: build state:modified+ only, deferring the rest to the production state
	mkdir -p $(CI_DIR) && rm -f $(CI_DIR)/pr.duckdb
	DBT_DEFER_DUCKDB_PATH=$(CI_STATE)/warehouse.duckdb $(DBT) build $(DBT_ARGS) --target ci \
		--select state:modified+ --defer --state $(abspath $(CI_STATE))

ci-logs: .env ## CI: print the last lines of every service log (no follow, for failed runs)
	$(COMPOSE) logs --no-color --tail=200

reset: .env ## Stop the stack and DELETE all data volumes
	$(COMPOSE) down -v
