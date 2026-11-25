"""The marts copied to Postgres for Evidence (ADR 0018), in a throwaway database."""

import uuid

import duckdb
import psycopg
import pytest
from psycopg import sql

from serving.publish import READER, _dsn, publish

pytestmark = pytest.mark.integration


@pytest.fixture
def database(env):
    name = f"analytics_test_{uuid.uuid4().hex[:8]}"
    yield name
    with psycopg.connect(_dsn(env, "lakekeeper"), autocommit=True) as conn:
        conn.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
        )


def _warehouse(path, rows: int):
    con = duckdb.connect(str(path))
    con.execute(f"create or replace table fct_x as select range as id from range({rows})")
    con.execute("create or replace table dim_y as select 1 as id")
    con.execute("create or replace table stg_z as select 1 as id")  # not a mart
    con.close()


def _reader(env, database):
    password = env.get("ANALYTICS_READER_PASSWORD", "analytics-reader-dev")
    host = env.get("PLATFORM_POSTGRES_HOST", "localhost")
    port = env.get("PLATFORM_POSTGRES_PORT", "5433")
    return psycopg.connect(
        f"host={host} port={port} dbname={database} user={READER} password={password}"
    )


def test_publish_replaces_the_marts_and_is_read_only(env, database, tmp_path):
    warehouse = tmp_path / "warehouse.duckdb"
    _warehouse(warehouse, rows=3)
    assert publish(env, warehouse, "2026-02-01T00:00:00+00:00", database).tables == {
        "dim_y": 1,
        "fct_x": 3,
    }

    _warehouse(warehouse, rows=5)
    publish(env, warehouse, "2026-02-02T00:00:00+00:00", database)

    with _reader(env, database) as conn:
        assert conn.execute("select count(*) from marts.fct_x").fetchone() == (5,)
        assert conn.execute("select simulated_now from marts._published").fetchall() == [
            ("2026-02-02T00:00:00+00:00",)
        ]
        tables = conn.execute(
            "select table_schema, table_name from information_schema.tables "
            "where table_schema like 'marts%' order by 2"
        ).fetchall()
        assert tables == [("marts", "_published"), ("marts", "dim_y"), ("marts", "fct_x")]
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute("create table marts.oops (id int)")


def test_alerts_are_kept_next_to_the_marts_and_readable_by_the_dashboard(env, database, tmp_path):
    """ADR 0026: the alerts table exists from the first publish, and alerts survive a publish."""
    from datetime import date

    from serving.alerts import raise_alert

    warehouse = tmp_path / "warehouse.duckdb"
    _warehouse(warehouse, rows=1)
    publish(env, warehouse, None, database)
    with _reader(env, database) as conn:
        assert conn.execute("select count(*) from ops.pipeline_alerts").fetchone() == (0,)

    raise_alert(env, "freshness", "fct_x", "behind", "warn", date(2026, 2, 3), database)
    publish(env, warehouse, None, database)  # replaces the marts, not the alerts
    with _reader(env, database) as conn:
        assert conn.execute(
            "select simulated_day, level, kind, subject from ops.pipeline_alerts"
        ).fetchall() == [(date(2026, 2, 3), "warn", "freshness", "fct_x")]
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute("delete from ops.pipeline_alerts")
