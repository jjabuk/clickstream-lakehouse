.DEFAULT_GOAL := help

.PHONY: help setup hooks lint format format-check test check clean \
	up down ps logs storage-init reset psql-shop psql-platform catalog-init hosts-check bootstrap test-integration day dbt-deps dbt-debug dbt-build

-include .env
export

COMPOSE := docker compose -f infra/docker-compose.yml --env-file .env
DBT := uv run dbt
DBT_ARGS := --project-dir dbt --profiles-dir dbt

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Install dependencies from uv.lock and git hooks
	uv sync --frozen
	$(MAKE) hooks

hooks: ## Install pre-commit git hooks
	uv run pre-commit install

lint: ## Lint with ruff
	uv run ruff check .

format: ## Format with ruff
	uv run ruff format .

format-check: ## Check formatting without changing files
	uv run ruff format --check .

test: ## Run unit tests (no stack needed)
	uv run pytest -m "not integration"

test-integration: ## Run smoke tests against the running stack
	uv run pytest -m integration

check: lint format-check test ## Run everything CI runs (lint, format check, tests)

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

reset: .env ## Stop the stack and DELETE all data volumes
	$(COMPOSE) down -v
