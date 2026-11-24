"""Apply one day to the source systems: the clickstream and the shop's Postgres.

The clickstream goes to one of two places. The default is a JSONL landing file, which
`python -m ingest` loads into Iceberg. With `stream=True` the events go to Kafka instead, and the
Iceberg sink writes them (ADR 0005). The orders in Postgres are the same either way.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

from generator import shop_db
from generator.batch import build_batch, simulate_day, to_jsonl
from generator.catalog import build_catalog
from generator.config import GeneratorConfig
from generator.faults import Fault, apply_fault
from generator.stream import TOPIC, produce
from generator.visitors import build_visitor_pool
from ingest import landing


@dataclass(frozen=True)
class DayReport:
    day: date
    events: int
    landing_path: str
    orders_created: int
    status_changes: int


def run_day(
    config: GeneratorConfig,
    day: date,
    env: Mapping[str, str],
    *,
    prefix: str = landing.DEFAULT_PREFIX,
    pg_schema: str | None = None,
    fault: Fault | None = None,
    stream: bool = False,
) -> DayReport:
    if day < config.epoch:
        raise ValueError(f"{day} is before the start of the simulated world ({config.epoch})")
    catalog = build_catalog(config)
    pool = build_visitor_pool(config)

    rows = apply_fault(build_batch(config, catalog, pool, day), fault)
    if stream:
        produce(rows, env)
        path = f"kafka://{TOPIC}"
    else:
        path = landing.landing_path(env, day, prefix)
        landing.write(landing.filesystem(env), path, to_jsonl(rows))

    orders = simulate_day(config, catalog, pool, day).orders
    with shop_db.connect(env, pg_schema) as conn:
        shop_db.ensure_schema(conn)
        shop_db.sync_reference(conn, catalog)
        shop_db.sync_users(conn, config, pool, day)
        created = shop_db.insert_orders(conn, orders)
        changes = shop_db.advance_orders(conn, config, day)
    return DayReport(day, len(rows), path, created, changes)
