"""The Iceberg operations of week 3 (ADR 0016) on a throwaway table, read back through DuckDB."""

from datetime import date, timedelta

import pytest

from generator.config import GeneratorConfig
from generator.day import run_day
from ingest import landing
from ingest.bronze import TABLE_NAME, load_day
from ingest.maintenance import (
    compact,
    data_files,
    expire,
    fragment,
    fragmented_days,
    partition_grain,
    set_partition_grain,
    snapshot_as_of,
)

pytestmark = pytest.mark.integration

DAY = date(2026, 1, 15)
CONFIG = GeneratorConfig(seed=11, n_visitors=300, sessions_per_day=300, bot_sessions_per_day=1)


@pytest.fixture
def bronze(env, catalog, namespace, sandbox):
    """A loader for days of a throwaway bronze table, and the table's identifier."""
    prefix, schema = sandbox
    ident = f"{namespace}.{TABLE_NAME}"

    def load(day):
        report = run_day(CONFIG, day, env, prefix=prefix, pg_schema=schema)
        load_day(catalog, landing.filesystem(env), report.landing_path, day, namespace)
        return catalog.load_table(ident)

    return load, ident


def rows(duck, ident, at=None):
    version = f" AT (VERSION => {at})" if at else ""
    return duck.execute(
        f"SELECT * FROM lk.{ident}{version} ORDER BY event_id, server_ts"
    ).fetchall()


def test_partition_evolution_keeps_old_files_and_readers_see_both(bronze, duck):
    load, ident = bronze
    table = load(DAY)
    assert set_partition_grain(table, "hour")
    assert not set_partition_grain(table.refresh(), "hour")  # idempotent
    table = load(DAY + timedelta(days=1))

    files = data_files(table)
    old = [f for f in files if f.arrival_day == DAY]
    new = [f for f in files if f.arrival_day == DAY + timedelta(days=1)]
    assert len(old) == 1 and old[0].spec_id == 0  # untouched
    assert len(new) > 1 and {f.spec_id for f in new} == {table.spec().spec_id}
    assert fragmented_days(files) == []  # one file per hour is not fragmentation

    days = duck.execute(
        f"SELECT CAST(server_ts AS DATE), count(*) FROM lk.{ident} GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert [d for d, _ in days] == [DAY, DAY + timedelta(days=1)]

    set_partition_grain(table.refresh(), "day")  # and back, still metadata only
    assert partition_grain(table.refresh()) == "day"
    load(DAY + timedelta(days=2))
    assert (
        len(duck.execute(f"SELECT DISTINCT CAST(server_ts AS DATE) FROM lk.{ident}").fetchall())
        == 3
    )


def test_compaction_merges_a_fragmented_day_without_changing_a_row(bronze, duck):
    load, ident = bronze
    load(DAY)
    table = load(DAY + timedelta(days=1))
    assert fragment(table, DAY, parts=12) == 12
    table = table.refresh()
    before = table.current_snapshot().snapshot_id
    assert fragmented_days(data_files(table)) == [DAY]

    (result,) = compact(table)
    assert (result.day, result.files_before, result.files_after) == (DAY, 12, 1)
    assert rows(duck, ident) == rows(duck, ident, at=before)


def test_time_travel_shows_a_day_before_its_late_events_arrived(bronze, duck):
    load, ident = bronze
    table = load(DAY)
    as_loaded = table.current_snapshot().snapshot_id
    table = load(DAY + timedelta(days=1))
    assert snapshot_as_of(table, DAY) == as_loaded

    query = f"SELECT count(*) FROM lk.{ident}{{at}} WHERE CAST(event_ts AS DATE) = '{DAY}'"
    then = duck.execute(query.format(at=f" AT (VERSION => {as_loaded})")).fetchone()[0]
    now = duck.execute(query.format(at="")).fetchone()[0]
    assert now > then  # the next day brought late events of DAY


def test_expiry_ends_time_travel_but_not_the_table_or_the_next_load(bronze, duck):
    load, ident = bronze
    table = load(DAY)
    first = table.current_snapshot().snapshot_id
    load(DAY + timedelta(days=1))
    table = load(DAY + timedelta(days=2))
    current = rows(duck, ident)

    result = expire(table, keep=1)
    assert first in result.expired
    assert result.unreferenced_files == 0  # every day's file is still live
    with pytest.raises(Exception, match="snapshot"):
        duck.execute(f"SELECT count(*) FROM lk.{ident} AT (VERSION => {first})").fetchall()
    assert rows(duck, ident) == current
    load(DAY + timedelta(days=3))  # write-audit-publish needs only the current snapshot
