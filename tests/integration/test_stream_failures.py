"""Failure tests for the stream into bronze (ADR 0021): break it at one point, check one invariant.

The sink's claim is that the rows and the Kafka offsets they reach are one Iceberg commit, so no
message is lost or written twice, whatever fails and wherever. Each test breaks the stream at a
specific point and then checks `assert_consistent`:

1. every message of the topic is on the branch head exactly once (by partition and offset, which
   `_source_file` records: an event_id can repeat on purpose, a message cannot);
2. the offsets at the head are the end of the topic;
3. main is an ancestor of `stream`, so publishing stays a fast-forward.

Each test has its own topic and table, so the dev data and the running sink are not touched.
Needs the stack with the stream profile (`make stream-up`); `make test-stream` runs them.
"""

import contextlib
import os
import random
import subprocess
import sys
import time
import uuid
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import SerializationError
from pyiceberg.exceptions import CommitFailedException

from generator.stream import produce
from ingest import stream
from ingest.bronze import ensure_table
from ingest.maintenance import StreamPending, compact, expire

pytestmark = [pytest.mark.integration, pytest.mark.stream]

ROOT = Path(__file__).resolve().parents[2]
DAY = date(2026, 3, 2)
PARTITIONS = 3


# --- fixtures -------------------------------------------------------------------------------------


def _bootstrap(env) -> str:
    return env.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")


def _registry(env) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": env.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")})


def create_topic(admin: AdminClient, name: str, partitions: int, timeout_s: float = 60) -> None:
    """Create the topic, waiting out a deletion of the same name that Kafka has not finished."""
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            admin.create_topics([NewTopic(name, partitions, 1)])[name].result(30)
            return
        except KafkaException:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


@pytest.fixture
def topic(env, request):
    """A topic of its own, three partitions unless the test asks for another number."""
    name = f"test_{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": _bootstrap(env)})
    create_topic(admin, name, getattr(request, "param", PARTITIONS))
    yield name
    admin.delete_topics([name])[name].result(30)
    for permanent in (False, True):
        with contextlib.suppress(SchemaRegistryError):  # gone already, or never registered
            _registry(env).delete_subject(f"{name}-value", permanent=permanent)


@pytest.fixture
def table(catalog, namespace):
    return stream.ensure_branch(ensure_table(catalog, namespace))


@pytest.fixture
def deserialize(env):
    return AvroDeserializer(_registry(env))


@pytest.fixture
def consumer(env):
    c = stream._consumer(env)
    yield c
    c.close()


# --- helpers --------------------------------------------------------------------------------------


def events(n: int, start: int = 0, day: date = DAY, utm: bool = False) -> list[dict]:
    """Valid page views, one second apart, with distinct event ids."""
    base = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=8)
    rows = []
    for i in range(start, start + n):
        ts = base + timedelta(seconds=i)
        row = {
            "event_id": f"{day:%Y%m%d}-{i:06d}",
            "event_type": "page_view",
            "event_ts": ts,
            "server_ts": ts,
            "cookie_id": f"cookie-{i % 40}",
            "user_id": None,
            "country": "PL",
            "device": "desktop",
            "user_agent": "failure-test",
            "page_type": "home",
            "page_url": "/",
            "product_id": None,
            "quantity": None,
        }
        if utm:
            row["utm_campaign"] = "spring"
        rows.append(row)
    return rows


def drain(table, consumer, deserialize, topic, max_rows=100) -> int:
    """Run the sink until it has nothing left to read. Returns the rows committed."""
    total = 0
    while rows := stream.sink_once(table, consumer, deserialize, max_rows, topic):
        total += rows
    return total


def topic_end(env, topic) -> dict[int, int]:
    c = stream._consumer(env)
    try:
        return stream.end_offsets(c, topic)
    finally:
        c.close()


def messages_on_branch(table, topic) -> Counter:
    head = stream.branch_head(table)
    sources = (
        table.scan(snapshot_id=head.snapshot_id, selected_fields=("_source_file",))
        .to_arrow()["_source_file"]
        .to_pylist()
    )
    prefix = f"kafka://{topic}/"
    return Counter(
        tuple(int(x) for x in s.removeprefix(prefix).split("/"))
        for s in sources
        if s.startswith(prefix)
    )


def assert_consistent(table, env, topic, skipped: frozenset = frozenset()) -> None:
    """The invariant: every message once, offsets at the end of the topic, main an ancestor."""
    table = table.refresh()
    end = topic_end(env, topic)
    reached = stream.offsets_at(table, stream.branch_head(table))
    assert {p: reached.get(p, 0) for p in end} == end, "the branch is not at the end of the topic"
    stream.ahead_of_main(table)  # raises unless main is an ancestor of the branch

    on_branch = messages_on_branch(table, topic)
    twice = sorted(m for m, n in on_branch.items() if n > 1)
    assert not twice, f"{len(twice)} messages written more than once, e.g. {twice[:3]}"
    expected = {(p, o) for p, n in end.items() for o in range(n)} - skipped
    missing, extra = expected - set(on_branch), set(on_branch) - expected
    assert not missing, f"{len(missing)} messages lost, e.g. {sorted(missing)[:3]}"
    assert not extra, f"{len(extra)} unexpected messages, e.g. {sorted(extra)[:3]}"


# --- 1. the sink fails after reading, before its commit -------------------------------------------


def test_a_sink_that_fails_before_its_commit_loses_nothing(
    env, table, consumer, deserialize, topic, monkeypatch
):
    produce(events(500), env, topic)
    before = stream.branch_head(table.refresh()).snapshot_id

    def crash(*_):
        raise RuntimeError("the sink died after reading from Kafka")

    monkeypatch.setattr(stream, "conform", crash)
    with pytest.raises(RuntimeError, match="sink died"):
        stream.sink_once(table, consumer, deserialize, 200, topic)
    assert stream.branch_head(table.refresh()).snapshot_id == before  # nothing committed

    monkeypatch.undo()
    assert drain(table, consumer, deserialize, topic) == 500  # the same messages, read again
    assert_consistent(table, env, topic)


# --- 2. the sink process is killed at random points -----------------------------------------------

SINK = (
    "import os, sys; from ingest.stream import run_sink; "
    "run_sink(os.environ, int(sys.argv[1]), 0.2, sys.argv[2], sys.argv[3])"
)


def test_a_sink_killed_again_and_again_loses_and_repeats_nothing(env, table, topic, namespace):
    """SIGKILL at random moments, mid-commit included: the commit is atomic, or it never was."""
    produce(events(1500), env, topic)
    end = topic_end(env, topic)
    rng = random.Random(42)  # the kill times are repeatable

    def start():
        return subprocess.Popen(
            [sys.executable, "-c", SINK, "50", topic, namespace],
            cwd=ROOT,
            env={**os.environ, **env},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def done():
        t = table.refresh()
        reached = stream.offsets_at(t, stream.branch_head(t))
        return all(reached.get(p, 0) >= o for p, o in end.items())

    kills = 0
    while not done() and kills < 40:
        proc = start()
        time.sleep(rng.uniform(1.5, 4.0))
        proc.kill()  # SIGKILL: no cleanup, no finally
        proc.wait()
        kills += 1
    assert kills >= 3, "the sink finished before it could be killed; produce more events"

    proc = start()  # a last, undisturbed run finishes the topic
    deadline = time.monotonic() + 120
    while not done() and time.monotonic() < deadline:
        time.sleep(1)
    proc.kill()
    proc.wait()
    assert_consistent(table, env, topic)


# --- 3. two sinks at once -------------------------------------------------------------------------


def _race(monkeypatch, other_sink):
    """Run `other_sink` once, after the sink under test has read its batch and before it commits."""
    real = stream.evolve_schema
    fired: list = []

    def racing(table, data):
        if not fired:
            fired.append(None)  # set first: the other sink passes through here too
            fired[0] = other_sink()
        return real(table, data)

    monkeypatch.setattr(stream, "evolve_schema", racing)
    return fired


def _second_sink(env, catalog, table, deserialize, topic, max_rows):
    other_table = catalog.load_table(table.name())  # its own view of the table
    other_consumer = stream._consumer(env)
    return lambda: stream.sink_once(other_table, other_consumer, deserialize, max_rows, topic)


def test_of_two_sinks_reading_the_same_batch_the_later_commit_fails(
    env, catalog, table, consumer, deserialize, topic, monkeypatch
):
    produce(events(400), env, topic)
    fired = _race(monkeypatch, _second_sink(env, catalog, table, deserialize, topic, 200))
    with pytest.raises(CommitFailedException):
        stream.sink_once(table, consumer, deserialize, 200, topic)
    assert fired == [200]  # the other sink committed the same messages first

    monkeypatch.undo()
    drain(table, consumer, deserialize, topic)  # reads on from the other sink's commit
    assert_consistent(table, env, topic)


@pytest.mark.parametrize("topic", [1], indirect=True)  # one partition: batches in offset order
def test_a_new_column_does_not_let_a_stale_batch_through(
    env, catalog, table, consumer, deserialize, topic, monkeypatch
):
    """Adding a column refreshes the table; the batch must still be checked against its start."""
    produce(events(100), env, topic)
    produce(events(100, start=100, utm=True), env, topic)  # utm_campaign appears at offset 100
    # The other sink commits offsets 0-99, which add no column; this one has read 0-199.
    fired = _race(monkeypatch, _second_sink(env, catalog, table, deserialize, topic, 100))
    with pytest.raises(CommitFailedException):
        stream.sink_once(table, consumer, deserialize, 200, topic)
    assert fired == [100]

    monkeypatch.undo()
    drain(table, consumer, deserialize, topic)
    assert_consistent(table, env, topic)


# --- 5. the topic is recreated --------------------------------------------------------------------


def test_a_recreated_topic_stops_the_sink(env, table, consumer, deserialize, topic):
    produce(events(300), env, topic)
    drain(table, consumer, deserialize, topic)
    admin = AdminClient({"bootstrap.servers": _bootstrap(env)})
    admin.delete_topics([topic])[topic].result(30)
    create_topic(admin, topic, partitions=PARTITIONS)
    produce(events(10, start=300), env, topic)  # offsets start again at 0
    head = stream.branch_head(table.refresh()).snapshot_id

    with pytest.raises(RuntimeError, match="was the topic recreated"):
        stream.sink_once(table, consumer, deserialize, 100, topic)
    assert stream.branch_head(table.refresh()).snapshot_id == head  # nothing skipped or re-read


# --- 6, 7. maintenance while a window waits -------------------------------------------------------


def test_expiry_keeps_the_commits_waiting_on_the_branch(env, table, consumer, deserialize, topic):
    """The regression of 2026-11-24: a scheduled expiry broke the fast-forward."""
    produce(events(600), env, topic)
    drain(table, consumer, deserialize, topic, max_rows=100)  # six sink commits, none on main
    expire(table.refresh(), keep=1)

    assert stream.publish_day(table, env, DAY, 30, topic) == 600
    table = table.refresh()
    assert table.current_snapshot().snapshot_id == stream.branch_head(table).snapshot_id
    assert_consistent(table, env, topic)


def test_compaction_waits_while_the_branch_has_unpublished_rows(
    env, table, consumer, deserialize, topic
):
    produce(events(300), env, topic)
    drain(table, consumer, deserialize, topic)
    with pytest.raises(StreamPending):
        compact(table.refresh(), days=[DAY])
    assert_consistent(table, env, topic)


def test_a_sink_commit_during_compaction_fails_the_compaction(
    env, catalog, table, consumer, deserialize, topic
):
    produce(events(600), env, topic)
    drain(table, consumer, deserialize, topic, max_rows=100)
    stream.publish_day(table, env, DAY, 30, topic)  # main now has six small files for DAY
    # Compaction's own view, taken while the branch is at main (refresh() updates in place).
    stale = catalog.load_table(table.name())
    main_before = stale.current_snapshot().snapshot_id

    produce(events(100, day=DAY + timedelta(days=1)), env, topic)
    drain(table, consumer, deserialize, topic)  # the sink commits in between
    with pytest.raises(CommitFailedException):
        compact(stale, days=[DAY])

    table = table.refresh()
    assert table.current_snapshot().snapshot_id == main_before
    assert_consistent(table, env, topic)  # the sink's commit is still on the branch


# --- 8. publish run twice -------------------------------------------------------------------------


def test_publishing_a_day_twice_changes_nothing(env, table, consumer, deserialize, topic):
    produce(events(300), env, topic)
    drain(table, consumer, deserialize, topic)
    assert stream.publish_day(table, env, DAY, 30, topic) == 300
    main = table.refresh().current_snapshot().snapshot_id

    assert stream.publish_day(table, env, DAY, 30, topic) == 0  # a retry finds nothing new
    table = table.refresh()
    assert table.current_snapshot().snapshot_id == main
    assert stream.rejected_tag(DAY) not in table.refs()
    assert_consistent(table, env, topic)


# --- 9. a message that cannot be decoded ----------------------------------------------------------


def test_an_undecodable_message_stops_the_sink_instead_of_being_skipped(
    env, table, consumer, deserialize, topic
):
    produce(events(100), env, topic)
    producer = Producer({"bootstrap.servers": _bootstrap(env)})
    producer.produce(topic, value=b"not avro")
    producer.flush(10)
    produce(events(100, start=100), env, topic)
    head = stream.branch_head(table.refresh()).snapshot_id

    for _ in range(2):  # it stops again on every try: the offset never moves past the message
        with pytest.raises(SerializationError):
            stream.sink_once(table, consumer, deserialize, 500, topic)
        assert stream.branch_head(table.refresh()).snapshot_id == head
