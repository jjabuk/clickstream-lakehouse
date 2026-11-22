"""Week 1 acceptance: one command's worth of traffic lands in Iceberg and in Postgres.

Runs against the real stack but in throwaway resources (Iceberg namespace, landing prefix and
Postgres schema), so it never touches the data that `make day` produced.
"""

from datetime import UTC, date, datetime, time, timedelta

import pytest

from generator import shop_db
from generator.batch import simulate_day
from generator.catalog import build_catalog
from generator.config import GeneratorConfig
from generator.day import run_day
from generator.orders import timeline
from generator.visitors import build_visitor_pool
from ingest import landing
from ingest.bronze import TABLE_NAME, load_day

pytestmark = pytest.mark.integration

DAY = date(2026, 1, 15)
CONFIG = GeneratorConfig(
    seed=9,
    n_visitors=400,
    sessions_per_day=400,
    bot_sessions_per_day=8,
    utm_start=DAY + timedelta(days=1),
)


def run_and_load(env, catalog, namespace, prefix, schema, day, config=CONFIG):
    report = run_day(config, day, env, prefix=prefix, pg_schema=schema)
    result = load_day(catalog, landing.filesystem(env), report.landing_path, day, namespace)
    return report, result


def bronze_count(duck, namespace, day):
    duck.execute("SET TimeZone = 'UTC'")
    return duck.execute(
        f"SELECT count(*) FROM lk.{namespace}.{TABLE_NAME} WHERE CAST(server_ts AS DATE) = ?", [day]
    ).fetchone()[0]


def test_a_day_lands_in_iceberg_and_postgres(env, catalog, namespace, duck, sandbox):
    prefix, schema = sandbox
    report, result = run_and_load(env, catalog, namespace, prefix, schema, DAY)

    # Iceberg: every delivered row is there, duplicates and late rows included.
    assert result.rows == report.events > 1000
    assert bronze_count(duck, namespace, DAY) == report.events
    distinct, ts_before_day = duck.execute(
        f"SELECT count(DISTINCT event_id), count(*) FILTER (WHERE event_ts < DATE '{DAY}') "
        f"FROM lk.{namespace}.{TABLE_NAME}"
    ).fetchone()
    assert distinct < report.events  # duplicates are kept in bronze
    assert ts_before_day > 0  # events from earlier days arrived late into this batch

    # Postgres: the day's orders, with their items, and the users they belong to.
    expected = simulate_day(CONFIG, build_catalog(CONFIG), build_visitor_pool(CONFIG), DAY).orders
    assert expected and report.orders_created == len(expected)
    with shop_db.connect(env, schema) as conn:
        n_orders, total = conn.execute("SELECT count(*), sum(total_cents) FROM orders").fetchone()
        assert n_orders == len(expected)
        assert total == sum(o.total_cents for o in expected)
        item_total = conn.execute(
            "SELECT sum(quantity * unit_price_cents) FROM order_items"
        ).fetchone()[0]
        assert item_total == total
        assert conn.execute("SELECT count(*) FROM users").fetchone()[0] > 0
        assert conn.execute("SELECT count(*) FROM products").fetchone()[0] == CONFIG.n_products


def test_running_a_day_twice_changes_nothing(env, catalog, namespace, duck, sandbox):
    prefix, schema = sandbox
    first, _ = run_and_load(env, catalog, namespace, prefix, schema, DAY)
    with shop_db.connect(env, schema) as conn:
        snapshot = conn.execute(
            "SELECT order_id, status, updated_at FROM orders ORDER BY order_id"
        ).fetchall()

    second, _ = run_and_load(env, catalog, namespace, prefix, schema, DAY)
    assert second.events == first.events
    assert bronze_count(duck, namespace, DAY) == first.events
    assert second.orders_created == 0 and second.status_changes == 0
    with shop_db.connect(env, schema) as conn:
        assert (
            conn.execute(
                "SELECT order_id, status, updated_at FROM orders ORDER BY order_id"
            ).fetchall()
            == snapshot
        )


def test_orders_move_through_statuses_as_days_pass(env, catalog, namespace, sandbox):
    """After N days every order has exactly the status its timeline prescribes for that moment."""
    prefix, schema = sandbox
    last_day = DAY + timedelta(days=5)
    for offset in range(0, 6):
        run_day(CONFIG, DAY + timedelta(days=offset), env, prefix=prefix, pg_schema=schema)
    horizon = datetime.combine(last_day + timedelta(days=1), time.min, tzinfo=UTC)

    with shop_db.connect(env, schema) as conn:
        rows = conn.execute(
            "SELECT order_id, created_at, status, updated_at FROM orders ORDER BY order_id"
        ).fetchall()
    assert len(rows) > 10
    for order_id, created_at, status, updated_at in rows:
        happened = [(n, ts) for n, ts in timeline(CONFIG, order_id, created_at) if ts < horizon]
        assert (status, updated_at) == happened[-1], order_id
    assert {r[2] for r in rows} >= {"shipped", "delivered"}  # the lifecycle actually advanced


def test_schema_drift_adds_utm_campaign_without_touching_old_rows(
    env, catalog, namespace, duck, sandbox
):
    prefix, schema = sandbox
    _, first = run_and_load(env, catalog, namespace, prefix, schema, DAY)
    assert first.new_columns == ()
    columns = [f.name for f in catalog.load_table(f"{namespace}.{TABLE_NAME}").schema().fields]
    assert "utm_campaign" not in columns

    next_day = DAY + timedelta(days=1)
    _, second = run_and_load(env, catalog, namespace, prefix, schema, next_day)
    assert second.new_columns == ("utm_campaign",)
    old_with_campaign, old_rows = duck.execute(
        f"SELECT count(utm_campaign), count(*) FROM lk.{namespace}.{TABLE_NAME} "
        f"WHERE CAST(server_ts AT TIME ZONE 'UTC' AS DATE) = DATE '{DAY}'"
    ).fetchone()
    assert old_rows > 0 and old_with_campaign == 0
    assert (
        duck.execute(f"SELECT count(utm_campaign) FROM lk.{namespace}.{TABLE_NAME}").fetchone()[0]
        > 0
    )
