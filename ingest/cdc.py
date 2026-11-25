"""The shop's change log into bronze: Debezium's topic in `bronze.shop_changes` (ADR 0007).

**Debezium** (the `connect` service, `infra/connect/shop-cdc.json`) reads Postgres's WAL and
writes every insert, update and delete of `orders`, `order_items` and `cdc_watermark` to one
topic, `shop.changes`, with one partition, so the log keeps the order in which Postgres committed
the changes, across tables. Its first start also snapshots the rows already there (`op = 'r'`):
the history of a row begins when CDC began.

**The sink** (`python -m ingest.cdc sink`, the `cdc-sink` service) appends the log to bronze as
it is, one row per change, with `before` and `after` as JSON text. It works like the click
stream's sink (ADR 0021): the Kafka offsets are committed in the Iceberg snapshot with the rows,
and a commit that finds the table moved fails and the batch is read again. There is no
write-audit-publish: a change log has no day to close, and a change is a fact about the source
whatever it looks like; staging decides what it means. Debezium itself delivers at least once (a
restart can send a change again), so staging deduplicates.

**The watermark** (`python -m ingest.cdc wait`) tells the pipeline that everything the shop
committed is in bronze. It inserts a row with a new id into `cdc_watermark` and waits until that
row arrives in bronze. The topic has one partition and Debezium follows the WAL, so every change
committed before the watermark has arrived too. The pattern is DBLog's (Netflix) and Debezium's
own incremental snapshots.
"""

import argparse
import json
import os
import time
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime

import pyarrow as pa
from confluent_kafka import Consumer
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, EqualTo
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import LongType, NestedField, StringType, TimestamptzType

from generator import shop_db
from ingest.bronze import NAMESPACE, NO_COMMIT_RETRY, lakehouse_catalog
from ingest.stream import OFFSETS, _consumer, check_not_ahead, end_offsets, offsets_at, read_batch

TOPIC = "shop.changes"
TABLE_NAME = "shop_changes"
WATERMARK = "cdc_watermark"
# ByLogicalTableRouter adds the source table to the key, so keys stay unique in the shared topic.
ROUTER_KEY_FIELD = "__dbz__physicalTableIdentifier"

SCHEMA = Schema(
    NestedField(1, "schema_name", StringType()),
    NestedField(2, "table_name", StringType()),
    NestedField(3, "op", StringType()),  # c(reate), u(pdate), d(elete), r(ead: the snapshot)
    NestedField(4, "key", StringType()),  # the primary key, as JSON
    NestedField(5, "before", StringType()),  # the row before the change, as JSON; NULL for c and r
    NestedField(6, "after", StringType()),  # the row after it, as JSON; NULL for d
    NestedField(7, "lsn", LongType()),  # the change's position in the WAL
    NestedField(8, "tx_id", LongType()),
    NestedField(9, "source_ts", TimestamptzType()),  # when Postgres committed it (wall clock)
    NestedField(10, "kafka_offset", LongType()),
    NestedField(11, "_ingested_at", TimestamptzType()),
)
ARROW_SCHEMA = SCHEMA.as_arrow()

WATERMARK_DDL = f"""
CREATE TABLE IF NOT EXISTS {WATERMARK} (
    watermark_id uuid PRIMARY KEY,
    written_at   timestamptz NOT NULL DEFAULT now()
)
"""


def ensure_table(catalog: Catalog, namespace: str = NAMESPACE) -> Table:
    catalog.create_namespace_if_not_exists(namespace)
    return catalog.create_table_if_not_exists(
        f"{namespace}.{TABLE_NAME}", schema=SCHEMA, properties=NO_COMMIT_RETRY
    )


def _json(value) -> str | None:
    return None if value is None else json.dumps(value, sort_keys=True)


def to_row(key: bytes | None, value: bytes, offset: int, now: datetime) -> dict:
    """One Debezium change event (JSON, schemas off) as a bronze row."""
    change = json.loads(value)
    source = change["source"]
    pk = json.loads(key) if key else {}
    pk.pop(ROUTER_KEY_FIELD, None)
    return {
        "schema_name": source["schema"],
        "table_name": source["table"],
        "op": change["op"],
        "key": _json(pk),
        "before": _json(change.get("before")),
        "after": _json(change.get("after")),
        "lsn": source.get("lsn"),
        "tx_id": source.get("txId"),
        "source_ts": datetime.fromtimestamp(source["ts_ms"] / 1000, UTC),
        "kafka_offset": offset,
        "_ingested_at": now,
    }


def sink_once(table: Table, consumer: Consumer, max_rows: int, topic: str = TOPIC) -> int:
    """Append the next batch from the offsets in the table's head. Returns the rows committed."""
    table = table.refresh()
    head = table.current_snapshot()
    start = offsets_at(table, head) if head else {}
    end = end_offsets(consumer, topic)
    check_not_ahead(start, end, topic, reader=TABLE_NAME)
    messages, reached = read_batch(consumer, topic, start, end, max_rows)
    now = datetime.now(UTC)
    rows = [to_row(m.key(), m.value(), m.offset(), now) for m in messages if m.value()]
    if not messages:
        return 0
    # No commit retry on the table: a concurrent commit makes this one fail, and the next round
    # reads again from the new head.
    table.append(
        pa.Table.from_pylist(rows, schema=ARROW_SCHEMA),
        snapshot_properties={OFFSETS: json.dumps(reached)},
    )
    return len(rows)


def _table_when_ready(env: Mapping[str, str], namespace: str) -> Table:
    """The sink starts with the stack, before `make bootstrap` has made the warehouse."""
    while True:
        try:
            return ensure_table(lakehouse_catalog(env), namespace)
        except Exception as not_ready:  # noqa: BLE001 - any failure here means "not yet"
            print(f"sink: waiting for the warehouse ({type(not_ready).__name__})", flush=True)
            time.sleep(10)


def run_sink(
    env: Mapping[str, str],
    max_rows: int,
    interval_s: float,
    topic: str = TOPIC,
    namespace: str = NAMESPACE,
) -> None:
    table = _table_when_ready(env, namespace)
    consumer = _consumer(env, group="bronze-cdc-sink")
    print(f"sink: {topic} -> {namespace}.{TABLE_NAME}", flush=True)
    while True:
        try:
            rows = sink_once(table, consumer, max_rows, topic)
        except CommitFailedException as conflict:
            print(f"sink: the table moved, reading again from its head ({conflict})", flush=True)
            continue
        if rows:
            print(f"sink: committed {rows} changes", flush=True)
        else:
            time.sleep(interval_s)


# --- the watermark -------------------------------------------------------------------------------


def write_watermark(env: Mapping[str, str], schema: str | None = None) -> uuid.UUID:
    """Insert a new watermark row in the shop's database (`schema` for tests) and return its id."""
    watermark = uuid.uuid4()
    with shop_db.connect(env, schema) as conn, conn.transaction():
        conn.execute(WATERMARK_DDL)
        conn.execute(f"INSERT INTO {WATERMARK} (watermark_id) VALUES (%s)", (watermark,))
    return watermark


def wait_for(table: Table, watermark: uuid.UUID, timeout_s: float, schema: str = "public") -> None:
    """Block until the watermark row is in bronze, and with it every change committed before it."""
    arrived = And(
        EqualTo("schema_name", schema),
        EqualTo("table_name", WATERMARK),
        EqualTo("key", _json({"watermark_id": str(watermark)})),
    )
    deadline = time.monotonic() + timeout_s
    while table.refresh().scan(row_filter=arrived).to_arrow().num_rows == 0:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"watermark {watermark} not in {NAMESPACE}.{TABLE_NAME} after {timeout_s:.0f} s. "
                "Are connect and cdc-sink running? See make cdc-status."
            )
        time.sleep(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="The shop's change log into bronze.")
    sub = parser.add_subparsers(dest="command", required=True)
    sink = sub.add_parser("sink", help="append the change log to bronze, forever")
    sink.add_argument("--max-rows", type=int, default=5000, help="changes per commit at most")
    sink.add_argument("--interval", type=float, default=2, help="seconds to wait when idle")
    wait = sub.add_parser("wait", help="write a watermark and wait until it is in bronze")
    wait.add_argument("--timeout", type=float, default=300, help="seconds to wait")
    latest = sub.add_parser("latest", help="print the latest changes in bronze")
    latest.add_argument("--n", type=int, default=20)
    args = parser.parse_args()

    env = os.environ
    if args.command == "sink":
        run_sink(env, args.max_rows, args.interval)
        return
    table = ensure_table(lakehouse_catalog(env))
    if args.command == "latest":
        rows = table.scan(
            selected_fields=("kafka_offset", "schema_name", "table_name", "op", "key", "after")
        ).to_arrow()
        for row in rows.sort_by([("kafka_offset", "descending")]).slice(0, args.n).to_pylist():
            after = (row["after"] or "")[:90]
            print(
                f"{row['kafka_offset']:>8} {row['schema_name']}.{row['table_name']:<14} "
                f"{row['op']} {row['key']} {after}"
            )
        return
    started = time.monotonic()
    watermark = write_watermark(env)
    wait_for(table, watermark, args.timeout)
    waited = time.monotonic() - started
    print(f"cdc: the shop's changes are in bronze (the watermark took {waited:.1f} s)")


if __name__ == "__main__":
    main()
