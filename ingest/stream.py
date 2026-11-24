"""The stream into bronze: a Kafka consumer that writes with PyIceberg, and write-audit-publish.

**The sink** (`python -m ingest.stream sink`, the `stream-sink` service, ADR 0005) reads the Avro
events from `clickstream.events` and appends them in small batches to the branch `stream` of
bronze, never to `main`. Each append records the Kafka offsets it reached in the snapshot summary
(`kafka.offsets`), in the same commit as the rows. So the branch is the consumer's checkpoint:
every batch starts at the offsets of the branch head, and a commit that finds the branch moved
fails and is read again from the new head. No row is lost or written twice, without Kafka
consumer groups or transactions.

**Write-audit-publish for the stream** (`publish --date`, ADR 0021) works like the file load
(ADR 0008), with a closed window instead of a file. The window is one arrival day:

1. **Closed window.** The generator has delivered the day (it returns once the broker has
   acknowledged every event), and the offsets at the head of `stream` equal the end of the topic:
   the sink has committed everything that was delivered.
2. **Audit.** Everything on `stream` after `main` must belong to the day. The day's rows on the
   branch then go through the same contract as a file (`ingest.audit`).
3. **Publish.** `main` is fast-forwarded to the head of `stream`.

A rejected window is tagged `rejected_YYYYMMDD` for inspection, and `stream` is reset to `main`
with an empty commit that records the topic's end offsets, so the sink skips the rejected
messages and the corrected redelivery starts from a clean branch. `main` never changes.

One window at a time: `make stream-day` and the DAG produce a day and publish it before the next.
"""

import argparse
import json
import os
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime

import pyarrow as pa
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Snapshot, ancestors_of
from pyiceberg.table.update.snapshot import ManageSnapshots

from generator.stream import TOPIC
from ingest.audit import AuditResult, audit_batch
from ingest.bronze import (
    NAMESPACE,
    TABLE_NAME,
    _volume_history,
    arrival_day_filter,
    conform,
    ensure_table,
    evolve_schema,
    lakehouse_catalog,
)

BRANCH = "stream"
OFFSETS = "kafka.offsets"  # snapshot summary: {partition: next offset to read}


# --- offsets -------------------------------------------------------------------------------------


def _consumer(env: Mapping[str, str]) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": env.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            # Offsets live in the table, not in Kafka: the group is never committed.
            "group.id": "bronze-stream-sink",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


def end_offsets(consumer: Consumer) -> dict[int, int]:
    partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
    return {
        p: consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)[1]
        for p in partitions
    }


def offsets_at(table: Table, snapshot: Snapshot) -> dict[int, int]:
    """The offsets of the newest sink commit in the snapshot's history; {} before the first.

    Loads and compactions carry none, so the walk goes back to the last sink commit.
    """
    for s in ancestors_of(snapshot, table.metadata):
        if OFFSETS in s.summary.additional_properties:
            return {int(p): o for p, o in json.loads(s.summary[OFFSETS]).items()}
    return {}


def check_not_ahead(reached: dict[int, int], end: dict[int, int]) -> None:
    """The branch cannot have read past the end of the topic, unless the topic was recreated."""
    ahead = {p: (o, end.get(p, 0)) for p, o in reached.items() if o > end.get(p, 0)}
    if ahead:
        raise RuntimeError(
            f"'{BRANCH}' has read past the end of {TOPIC} (partition: (branch, topic) {ahead}); "
            "was the topic recreated? Its offsets no longer match the branch."
        )


def branch_head(table: Table) -> Snapshot:
    return table.snapshot_by_name(BRANCH)


def ensure_branch(table: Table) -> Table:
    if BRANCH not in table.refs():
        main = table.current_snapshot().snapshot_id
        table.manage_snapshots().create_branch(main, BRANCH).commit()
        table = table.refresh()
    return table


# --- the sink ------------------------------------------------------------------------------------


def _to_arrow(messages, deserialize) -> pa.Table:
    rows = []
    now = datetime.now(UTC)
    for m in messages:
        row = deserialize(m.value(), SerializationContext(TOPIC, MessageField.VALUE))
        row["_source_file"] = f"kafka://{TOPIC}/{m.partition()}/{m.offset()}"
        row["_ingested_at"] = now
        rows.append(row)
    data = pa.Table.from_pylist(rows)
    # Avro's timestamp-millis arrives as datetimes; bronze stores microseconds in UTC.
    for name in ("event_ts", "server_ts", "_ingested_at"):
        data = data.set_column(
            data.schema.get_field_index(name), name, data[name].cast(pa.timestamp("us", "UTC"))
        )
    return data


def sink_once(table: Table, consumer: Consumer, deserialize, max_rows: int) -> int:
    """Append the next batch from the branch head's offsets. Returns the rows committed."""
    table = table.refresh()
    head = branch_head(table)
    start = offsets_at(table, head)
    end = end_offsets(consumer)
    check_not_ahead(start, end)
    if all(start.get(p, 0) >= o for p, o in end.items()):
        return 0
    consumer.assign([TopicPartition(TOPIC, p, start.get(p, 0)) for p in end])
    messages, reached = [], dict(start)
    while len(messages) < max_rows:
        batch = consumer.consume(min(500, max_rows - len(messages)), timeout=1)
        if not batch:
            break
        for m in batch:
            if m.error():
                raise RuntimeError(m.error())
            messages.append(m)
            reached[m.partition()] = m.offset() + 1
    if not messages:
        return 0
    data = _to_arrow(messages, deserialize)
    table, _ = evolve_schema(table, data)
    # The branch must still be where the batch started; otherwise the commit fails and the next
    # round reads again from the new head.
    table.append(
        conform(data, table),
        branch=BRANCH,
        snapshot_properties={OFFSETS: json.dumps(reached)},
    )
    return len(messages)


def run_sink(env: Mapping[str, str], max_rows: int, interval_s: float) -> None:
    table = ensure_branch(ensure_table(lakehouse_catalog(env)))
    registry = SchemaRegistryClient(
        {"url": env.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")}
    )
    deserialize = AvroDeserializer(registry)
    consumer = _consumer(env)
    print(f"sink: {TOPIC} -> {NAMESPACE}.{TABLE_NAME}@{BRANCH}", flush=True)
    while True:
        try:
            rows = sink_once(table, consumer, deserialize, max_rows)
        except CommitFailedException as conflict:
            print(f"sink: branch moved, reading again from its head ({conflict})", flush=True)
            continue
        if rows:
            print(f"sink: committed {rows} rows", flush=True)
        else:
            time.sleep(interval_s)


# --- write-audit-publish -------------------------------------------------------------------------


@dataclass(frozen=True)
class Window:
    head: Snapshot
    base: int
    committed_rows: int
    end_offsets: dict[int, int]


class StreamAuditFailed(Exception):
    def __init__(self, day: date, tag: str, result: AuditResult) -> None:
        self.day, self.tag, self.result = day, tag, result
        details = "; ".join(result.violations)
        super().__init__(f"{day}: audit failed, kept as tag '{tag}': {details}")


def rejected_tag(day: date) -> str:
    return f"rejected_{day:%Y%m%d}"


def ahead_of_main(table: Table) -> list[Snapshot]:
    """The snapshots on `stream` after `main`'s head. `main` must be an ancestor to fast-forward."""
    main = table.current_snapshot().snapshot_id
    ahead = []
    for snapshot in ancestors_of(branch_head(table), table.metadata):
        if snapshot.snapshot_id == main:
            return ahead
        ahead.append(snapshot)
    raise RuntimeError(f"main ({main}) is not an ancestor of '{BRANCH}'; it cannot fast-forward")


def wait_closed(table: Table, env: Mapping[str, str], timeout_s: float) -> Window:
    consumer = _consumer(env)
    try:
        end = end_offsets(consumer)
    finally:
        consumer.close()
    deadline = time.monotonic() + timeout_s
    while True:
        table = table.refresh()
        head = branch_head(table)
        reached = offsets_at(table, head)
        check_not_ahead(reached, end)
        if all(reached.get(p, 0) >= o for p, o in end.items()):
            ahead = ahead_of_main(table)
            return Window(
                head=head,
                base=table.current_snapshot().snapshot_id,
                committed_rows=sum(int(s.summary.get("added-records") or 0) for s in ahead),
                end_offsets=end,
            )
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"the window did not close in {timeout_s:.0f} s: the sink is at {reached}, "
                f"the topic ends at {end}. Is stream-sink running?"
            )
        time.sleep(2)


def publish_day(table: Table, env: Mapping[str, str], day: date, timeout_s: float = 300) -> int:
    """Audit the closed window of `day` on `stream` and publish it. Returns the rows published."""
    window = wait_closed(table, env, timeout_s)
    table = table.refresh()
    in_day = arrival_day_filter(day)

    # Everything after main must be this day's: the branch's rows of the day minus main's.
    day_on_branch = table.scan(row_filter=in_day, snapshot_id=window.head.snapshot_id).to_arrow()
    day_on_main = table.scan(row_filter=in_day).count()
    outside = window.committed_rows - (day_on_branch.num_rows - day_on_main)
    result = audit_batch(day_on_branch, _volume_history(table, day))
    if outside:
        result = AuditResult(
            result.rows, (*result.violations, f"{outside} rows after main are not from {day}")
        )

    if not result.passed:
        tag = rejected_tag(day)
        with table.transaction() as tx:
            ms = ManageSnapshots(tx)
            if tag in table.refs():
                ms = ms.remove_tag(tag)
            ms.create_tag(window.head.snapshot_id, tag).create_branch(window.base, BRANCH).commit()
            # The sink resumes after the rejected messages.
            tx.append(
                table.schema().as_arrow().empty_table(),
                branch=BRANCH,
                snapshot_properties={OFFSETS: json.dumps(window.end_offsets)},
            )
        raise StreamAuditFailed(day, tag, result)

    if window.committed_rows:
        table.manage_snapshots().set_current_snapshot(snapshot_id=window.head.snapshot_id).commit()
    return window.committed_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="The Kafka stream into bronze.")
    sub = parser.add_subparsers(dest="command", required=True)
    sink = sub.add_parser("sink", help="append the topic to the branch, forever")
    sink.add_argument("--max-rows", type=int, default=2000, help="rows per commit at most")
    sink.add_argument("--interval", type=float, default=5, help="seconds to wait when idle")
    publish = sub.add_parser("publish", help="audit the day's closed window and publish it")
    publish.add_argument("--date", required=True, type=date.fromisoformat)
    publish.add_argument("--timeout", type=float, default=300, help="seconds to wait")
    args = parser.parse_args()

    env = os.environ
    if args.command == "sink":
        run_sink(env, args.max_rows, args.interval)
        return
    table = ensure_branch(ensure_table(lakehouse_catalog(env)))
    try:
        rows = publish_day(table, env, args.date, args.timeout)
    except StreamAuditFailed as failure:
        print(
            f"{failure.day}: audit failed, main unchanged; the window is kept as tag "
            f"'{failure.tag}' ({failure.result.rows} rows) and '{BRANCH}' is reset:",
            file=sys.stderr,
        )
        for violation in failure.result.violations:
            print(f"  - {violation}", file=sys.stderr)
        sys.exit(1)
    print(f"{args.date}: {rows} streamed rows audited and published to {NAMESPACE}.{TABLE_NAME}")


if __name__ == "__main__":
    main()
