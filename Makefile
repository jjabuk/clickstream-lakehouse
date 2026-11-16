.DEFAULT_GOAL := help

.PHONY: help setup hooks lint format format-check test check clean \
	up down ps logs storage-init reset psql-shop psql-platform

COMPOSE := docker compose -f infra/docker-compose.yml --env-file .env

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

test: ## Run pytest
	uv run pytest

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

psql-shop: .env ## Open psql in the shop database (orders, CDC source)
	$(COMPOSE) exec postgres-shop sh -c 'psql -U "$$POSTGRES_USER" -d shop'

psql-platform: .env ## Open psql in the platform instance (Lakekeeper catalog)
	$(COMPOSE) exec postgres-platform sh -c 'psql -U "$$POSTGRES_USER" -d lakekeeper'

reset: .env ## Stop the stack and DELETE all data volumes
	$(COMPOSE) down -v
