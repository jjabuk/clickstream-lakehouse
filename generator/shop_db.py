"""The shop's Postgres database: reference data, users and orders (the CDC source from week 5).

Everything here is idempotent, so a day can be applied again after a failure or a re-run:
DDL is `IF NOT EXISTS`, inserts are `ON CONFLICT`, status transitions are derived from the
current state (see `orders.py`). Time in the database is synthetic: `updated_at` carries the
simulated time of the change, not the wall clock, so snapshots and the CDC log tell a coherent
story whatever day the generator is run on.
"""

from collections.abc import Mapping
from datetime import UTC, date, datetime, time, timedelta

import psycopg

from generator.catalog import Catalog
from generator.config import GeneratorConfig
from generator.orders import FINAL_STATUSES, MAX_LIFECYCLE_DAYS, Order, due_transitions, timeline
from generator.visitors import VisitorPool

DDL = """
CREATE TABLE IF NOT EXISTS categories (
    category_id integer PRIMARY KEY,
    slug        text NOT NULL
);
CREATE TABLE IF NOT EXISTS products (
    product_id  integer PRIMARY KEY,
    category_id integer NOT NULL REFERENCES categories,
    price_cents integer NOT NULL
);
CREATE TABLE IF NOT EXISTS users (
    user_id    bigint PRIMARY KEY,
    country    text NOT NULL,
    plan       text NOT NULL CHECK (plan IN ('free', 'premium')),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    order_id    bigint PRIMARY KEY,
    user_id     bigint REFERENCES users,
    cookie_id   uuid NOT NULL,
    status      text NOT NULL CHECK (status IN
        ('created', 'paid', 'shipped', 'delivered', 'returned', 'cancelled')),
    total_cents bigint NOT NULL,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS order_items (
    order_id         bigint NOT NULL REFERENCES orders,
    line_no          integer NOT NULL,
    product_id       integer NOT NULL REFERENCES products,
    quantity         integer NOT NULL,
    unit_price_cents integer NOT NULL,
    PRIMARY KEY (order_id, line_no)
);
-- Debezium (week 5) must see the previous row image to emit a useful `before`.
ALTER TABLE orders REPLICA IDENTITY FULL;
"""


def connect(env: Mapping[str, str], schema: str | None = None) -> psycopg.Connection:
    """Connect to the shop database; `schema` (used by tests) isolates all tables in it."""
    conn = psycopg.connect(
        host=env.get("SHOP_POSTGRES_HOST", "localhost"),
        port=int(env.get("SHOP_POSTGRES_PORT", "5432")),
        user=env["SHOP_POSTGRES_USER"],
        password=env["SHOP_POSTGRES_PASSWORD"],
        dbname="shop",
    )
    if schema is not None:
        with conn.transaction():
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            conn.execute(f'SET search_path TO "{schema}"')
    return conn


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute(DDL)


def sync_reference(conn: psycopg.Connection, catalog: Catalog) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO categories VALUES (%s, %s) "
            "ON CONFLICT (category_id) DO UPDATE SET slug = EXCLUDED.slug",
            [(c.category_id, c.slug) for c in catalog.categories],
        )
        cur.executemany(
            "INSERT INTO products VALUES (%s, %s, %s) ON CONFLICT (product_id) DO UPDATE "
            "SET category_id = EXCLUDED.category_id, price_cents = EXCLUDED.price_cents",
            [(p.product_id, p.category_id, p.price_cents) for p in catalog.products],
        )


def _midnight(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=UTC)


def sync_users(
    conn: psycopg.Connection, config: GeneratorConfig, pool: VisitorPool, day: date
) -> None:
    """Bring `users` to its state at the end of `day`.

    A row changes (and gets `updated_at` = the effective time of the latest change) only when an
    attribute differs, so unchanged users produce no UPDATE and no CDC noise.
    """
    rows = []
    for v in pool.visitors:
        if v.user_id is None:
            continue
        attrs = v.attributes_on(day)
        effective = [c.effective for c in v.changes if c.effective <= day]
        updated_at = _midnight(max(effective)) if effective else _midnight(config.epoch)
        rows.append(
            (v.user_id, attrs["country"], attrs["plan"], _midnight(config.epoch), updated_at)
        )
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO users VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE "
            "SET country = EXCLUDED.country, plan = EXCLUDED.plan, "
            "updated_at = EXCLUDED.updated_at "
            "WHERE (users.country, users.plan) IS DISTINCT FROM (EXCLUDED.country, EXCLUDED.plan)",
            rows,
        )


def insert_orders(conn: psycopg.Connection, orders: list[Order]) -> int:
    """Insert new orders in status `created`; orders that already exist are left alone."""
    inserted = 0
    with conn.transaction(), conn.cursor() as cur:
        for o in orders:
            cur.execute(
                "INSERT INTO orders VALUES (%s, %s, %s, 'created', %s, %s, %s) "
                "ON CONFLICT (order_id) DO NOTHING",
                (o.order_id, o.user_id, o.cookie_id, o.total_cents, o.created_at, o.created_at),
            )
            if cur.rowcount == 0:
                continue
            inserted += 1
            cur.executemany(
                "INSERT INTO order_items VALUES (%s, %s, %s, %s, %s)",
                [
                    (o.order_id, i + 1, pid, qty, price)
                    for i, (pid, qty, price) in enumerate(o.items)
                ],
            )
    return inserted


def advance_orders(conn: psycopg.Connection, config: GeneratorConfig, day: date) -> int:
    """Play every status transition due by the end of `day`, oldest first. Returns the count."""
    until = _midnight(day + timedelta(days=1))
    pending = conn.execute(
        "SELECT order_id, created_at, status FROM orders "
        "WHERE status <> ALL(%s) AND created_at >= %s",
        (sorted(FINAL_STATUSES), until - timedelta(days=MAX_LIFECYCLE_DAYS)),
    ).fetchall()
    updates: list[tuple[datetime, int, str]] = []
    for order_id, created_at, status in pending:
        steps = timeline(config, order_id, created_at)
        updates += [(ts, order_id, name) for name, ts in due_transitions(steps, status, until)]
    updates.sort()  # chronological, so the future change log is in the order things happened
    with conn.transaction(), conn.cursor() as cur:
        for ts, order_id, name in updates:
            cur.execute(
                "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
                (name, ts, order_id),
            )
    return len(updates)
