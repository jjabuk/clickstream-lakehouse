"""CDC end to end (ADR 0007): Postgres -> Debezium -> Kafka -> cdc-sink -> bronze.shop_changes.

Needs the core stack (`make bootstrap`), whose connector captures the shop's tables in every
schema. The generator writes to a throwaway schema, so the test's changes are told apart from
the dev data by `schema_name`; they are deleted from bronze afterwards.
"""

import json
from datetime import date, timedelta

import duckdb
import pytest
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo

from generator import shop_db
from generator.config import GeneratorConfig
from generator.day import run_day
from ingest.cdc import ensure_table, wait_for, write_watermark

pytestmark = pytest.mark.integration

DAY = date(2026, 1, 15)
CONFIG = GeneratorConfig(seed=11, n_visitors=300, sessions_per_day=300, bot_sessions_per_day=4)


@pytest.fixture
def changes(catalog, sandbox):
    table = ensure_table(catalog)
    yield table
    _, schema = sandbox
    for _ in range(5):  # the sink commits to the same table; a lost race is retried here
        try:
            table.refresh().delete(EqualTo("schema_name", schema))
            return
        except CommitFailedException:
            continue


def test_the_change_log_replays_to_the_state_in_postgres(env, sandbox, changes):
    prefix, schema = sandbox
    reports = [
        run_day(CONFIG, DAY + timedelta(days=d), env, prefix=prefix, pg_schema=schema)
        for d in range(3)
    ]
    wait_for(changes, write_watermark(env, schema), timeout_s=120, schema=schema)

    log = changes.refresh().scan(row_filter=EqualTo("schema_name", schema)).to_arrow()
    con = duckdb.connect()
    con.register("log", log)
    by_op = "SELECT op, count(*) FROM log WHERE table_name = 'orders' GROUP BY op"
    ops = dict(con.execute(by_op).fetchall())
    # Every insert and every status change, each once: the sink neither loses nor repeats one.
    assert ops == {
        "c": sum(r.orders_created for r in reports),
        "u": sum(r.status_changes for r in reports),
    }

    # The latest image of each order, in log order, is the row in Postgres.
    replayed = con.execute(
        """
        SELECT CAST(after ->> 'order_id' AS BIGINT), after ->> 'status'
        FROM log WHERE table_name = 'orders'
        QUALIFY row_number() OVER (PARTITION BY key ORDER BY kafka_offset DESC) = 1
        ORDER BY 1
        """
    ).fetchall()
    with shop_db.connect(env, schema) as conn:
        source = conn.execute("SELECT order_id, status FROM orders ORDER BY 1").fetchall()
    assert replayed == source

    # An update carries the previous image too (REPLICA IDENTITY FULL), so a transition is
    # readable from one row.
    before, after = con.execute(
        "SELECT before, after FROM log WHERE op = 'u' ORDER BY kafka_offset LIMIT 1"
    ).fetchone()
    assert json.loads(before)["status"] != json.loads(after)["status"]
