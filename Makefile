.DEFAULT_GOAL := help

.PHONY: help setup hooks lint format format-check test check clean

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
