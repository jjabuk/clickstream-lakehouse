"""Fixtures for tests that need the running local stack (`make bootstrap`)."""

import uuid
from pathlib import Path

import pytest
from pyiceberg.catalog import Catalog

from generator import shop_db
from ingest import landing
from ingest.bronze import lakehouse_catalog
from ingest.reader import lakehouse_duckdb

ROOT = Path(__file__).resolve().parents[2]


def _load_env() -> dict[str, str]:
    env_file = ROOT / ".env"
    if not env_file.exists():
        pytest.fail(".env missing: run `make bootstrap` before the integration tests")
    pairs = (
        line.split("=", 1)
        for line in env_file.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )
    return {k.strip(): v.strip() for k, v in pairs}


@pytest.fixture(scope="session")
def env() -> dict[str, str]:
    return _load_env()


@pytest.fixture(scope="session")
def catalog(env: dict[str, str]) -> Catalog:
    """The same catalog the ingest uses, so the tests exercise its S3 configuration."""
    return lakehouse_catalog(env)


@pytest.fixture
def sandbox(env):
    """A throwaway landing prefix and Postgres schema, removed after the test."""
    suffix = uuid.uuid4().hex[:8]
    prefix, schema = f"raw/test_{suffix}", f"test_{suffix}"
    yield prefix, schema
    fs = landing.filesystem(env)
    fs.delete_dir_contents(f"{env['S3_BUCKET']}/{prefix}", missing_dir_ok=True)
    with shop_db.connect(env) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture
def namespace(catalog: Catalog):
    """A throwaway namespace, purged after the test."""
    name = f"smoke_{uuid.uuid4().hex[:8]}"
    catalog.create_namespace(name)
    yield name
    for ident in catalog.list_tables(name):
        catalog.purge_table(ident)
    catalog.drop_namespace(name)


@pytest.fixture
def duck(env: dict[str, str]):
    """DuckDB attached to the catalog with its own S3 key (no vended credentials, ADR 0003)."""
    con = lakehouse_duckdb(env, read_only=False)  # test_stack also writes through DuckDB
    yield con
    con.close()
