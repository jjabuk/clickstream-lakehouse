"""Iceberg table operations on bronze, run by hand with `make iceberg-*` (ADR 0016).

Each operation is shown in the README with a query before and after. Schema evolution happens
during the load (`ingest.bronze`); this module covers the rest:

- **partition evolution:** `day(server_ts)` to `hour(server_ts)` and back. Metadata only: old
  files keep their spec, new writes use the new one, and readers prune on both;
- **time travel:** the table as it was before later arrival days brought late events;
- **compaction:** PyIceberg 0.12 has no `rewrite_data_files`, so a fragmented arrival day is
  rewritten in one overwrite. The stream sink leaves such days (a file per commit); `fragment`
  produces one on purpose. The Airflow DAG `iceberg_maintenance` compacts on a schedule;
- **snapshot expiration:** metadata only, and never the commits that wait on a branch for
  publishing. PyIceberg deletes no files and Lakekeeper does not
  either, so the files only expired snapshots used are reported, not removed.

The decisions are pure functions of table metadata, unit tested without the stack.
"""

import argparse
import os
import textwrap
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.table import Table
from pyiceberg.table.refs import SnapshotRefType
from pyiceberg.table.snapshots import Snapshot, ancestors_of
from pyiceberg.table.update.snapshot import ManageSnapshots
from pyiceberg.transforms import DayTransform, HourTransform

from ingest.bronze import NAMESPACE, TABLE_NAME, arrival_day_filter, lakehouse_catalog
from ingest.reader import lakehouse_duckdb
from ingest.stream import BRANCH as STREAM_BRANCH

GRAINS = {"day": DayTransform(), "hour": HourTransform()}
SOURCE_COLUMN = "server_ts"
# Written into the snapshot summary, so the history tells maintenance commits from loads.
OPERATION_PROPERTY = "maintenance"


# --- partition evolution -----------------------------------------------------------------------


def partition_grain(table: Table) -> str:
    (field,) = table.spec().fields
    return next(name for name, transform in GRAINS.items() if field.transform == transform)


def set_partition_grain(table: Table, grain: str) -> bool:
    """Evolve the spec to partition by `grain` of server_ts. Returns False if it already does."""
    current = partition_grain(table)
    if current == grain:
        return False
    with table.update_spec() as spec:
        spec.remove_field(f"{SOURCE_COLUMN}_{current}")
        spec.add_field(SOURCE_COLUMN, GRAINS[grain], f"{SOURCE_COLUMN}_{grain}")
    return True


# --- files and compaction ----------------------------------------------------------------------


@dataclass(frozen=True)
class DataFile:
    path: str
    spec_id: int
    partition: str
    arrival_day: date
    records: int
    size: int


def data_files(table: Table) -> list[DataFile]:
    """The live data files of main. A file never spans arrival days: each load writes one day."""
    if table.current_snapshot() is None:
        return []
    files = table.inspect.files().to_pylist()
    return [
        DataFile(
            path=f["file_path"],
            spec_id=f["spec_id"],
            partition=str(sorted((k, v) for k, v in f["partition"].items() if v is not None)),
            arrival_day=f["readable_metrics"][SOURCE_COLUMN]["lower_bound"].date(),
            records=f["record_count"],
            size=f["file_size_in_bytes"],
        )
        for f in files
        if f["content"] == 0 and f["record_count"]
    ]


def fragmented_days(files: Iterable[DataFile]) -> list[date]:
    """Arrival days with more than one file in the same partition: what compaction merges.

    Several files in *different* partitions (24 hours of a day under the hour spec) are not
    fragmentation; a rewrite would write the same files again.
    """
    per_partition: dict[tuple[date, int, str], int] = defaultdict(int)
    for f in files:
        per_partition[(f.arrival_day, f.spec_id, f.partition)] += 1
    return sorted({day for (day, _, _), n in per_partition.items() if n > 1})


@dataclass(frozen=True)
class Compaction:
    day: date
    files_before: int
    files_after: int


def _files_per_day(table: Table) -> dict[date, int]:
    counts: dict[date, int] = defaultdict(int)
    for f in data_files(table):
        counts[f.arrival_day] += 1
    return counts


class StreamPending(Exception):
    """The stream sink's branch holds rows that are not published yet; compaction waits."""


def compact(
    table: Table, days: Sequence[date] | None = None, follow: str = STREAM_BRANCH
) -> list[Compaction]:
    """Rewrite each fragmented arrival day (or the given ones) as one overwrite, in one commit.

    The rows are read back and written unchanged, `_ingested_at` included, into as few files as
    the current spec allows. Unlike Spark's `rewrite_data_files`, which commits a `replace`, this
    commits an `overwrite`: a reader that follows snapshots incrementally would see the day
    change. dbt reads bronze by `server_ts`, not by snapshot, so it is unaffected.

    The stream sink writes to the branch `follow`, which must stay a descendant of main so that
    publishing it is a fast-forward (ADR 0021). So compaction runs only while that branch is at
    main, and moves it to the compacted head in the same commit. If the sink commits in between,
    the whole commit fails and the next run tries again.
    """
    days = list(days) if days is not None else fragmented_days(data_files(table))
    if not days:
        return []
    branch = table.refs().get(follow)
    if branch is not None and branch.snapshot_id != table.current_snapshot().snapshot_id:
        raise StreamPending(f"'{follow}' has rows that are not published yet")
    before = _files_per_day(table)
    batches = {day: table.scan(row_filter=arrival_day_filter(day)).to_arrow() for day in days}
    with table.transaction() as tx:
        for day, rows in batches.items():
            tx.overwrite(
                rows,
                overwrite_filter=arrival_day_filter(day),
                snapshot_properties={OPERATION_PROPERTY: "compaction"},
            )
        if branch is not None:
            head = tx.table_metadata.current_snapshot_id
            ManageSnapshots(tx).create_branch(head, follow).commit()
    after = _files_per_day(table.refresh())
    return [Compaction(day, before[day], after[day]) for day in days]


def fragment(table: Table, day: date, parts: int) -> int:
    """Rewrite one arrival day as `parts` appends in time order, the small files a sink leaves.

    A demonstration aid for compaction, not part of the pipeline. One transaction, so readers
    never see a partial day. Returns the number of files the day now has.
    """
    rows = table.scan(row_filter=arrival_day_filter(day)).to_arrow()
    if not rows.num_rows:
        raise ValueError(f"{day} has no rows on main")
    rows = rows.take(pc.sort_indices(rows, sort_keys=[(SOURCE_COLUMN, "ascending")]))
    size = -(-rows.num_rows // parts)  # ceiling division
    chunks = [rows.slice(start, size) for start in range(0, rows.num_rows, size)]
    with table.transaction() as tx:
        props = {OPERATION_PROPERTY: "fragment"}
        tx.overwrite(chunks[0], overwrite_filter=arrival_day_filter(day), snapshot_properties=props)
        for chunk in chunks[1:]:
            tx.append(chunk, snapshot_properties=props)
    return _files_per_day(table.refresh())[day]


# --- time travel -------------------------------------------------------------------------------


def main_ancestry(table: Table) -> list[Snapshot]:
    """Snapshots of main, oldest first, as far back as the metadata still holds them."""
    return list(reversed(list(ancestors_of(table.current_snapshot(), table.metadata))))


def latest_added_day(table: Table, snapshot: Snapshot) -> date | None:
    """The latest arrival day among the files a snapshot added; None if it added none."""
    entries = table.inspect.entries(snapshot.snapshot_id)
    added = entries.filter(
        pc.and_(
            pc.equal(entries["status"], 1),  # ADDED
            pc.equal(entries["snapshot_id"], snapshot.snapshot_id),
        )
    )
    if not added.num_rows:
        return None
    metrics = added["readable_metrics"].combine_chunks().field(SOURCE_COLUMN).field("upper_bound")
    return pc.max(metrics).as_py().date()


@dataclass(frozen=True)
class HistoryEntry:
    snapshot_id: int
    latest_added_day: date | None
    maintenance: bool


def snapshot_before_later_days(history: Sequence[HistoryEntry], day: date) -> int | None:
    """The last snapshot of main after `day` was loaded and before any later day arrived.

    `history` is main's ancestry, oldest first. Maintenance commits (compaction, fragment) are
    skipped: they only rewrite days that already arrived, and the first half of a delete-then-
    append rewrite is not a state the table was ever published in. None if the load of `day` is
    no longer in the history, e.g. because its snapshot expired.
    """
    found, loaded = None, False
    for entry in history:
        if entry.maintenance:
            continue
        if entry.latest_added_day is not None and entry.latest_added_day > day:
            break
        loaded = loaded or entry.latest_added_day == day
        if loaded:
            found = entry.snapshot_id
    return found


def snapshot_as_of(table: Table, day: date) -> int | None:
    history = [
        HistoryEntry(
            s.snapshot_id,
            latest_added_day(table, s),
            bool(s.summary and OPERATION_PROPERTY in s.summary.additional_properties),
        )
        for s in main_ancestry(table)
    ]
    return snapshot_before_later_days(history, day)


# --- snapshot expiration -----------------------------------------------------------------------


def snapshots_to_expire(
    all_ids: Iterable[int], main_newest_first: Sequence[int], protected: set[int], keep: int
) -> list[int]:
    """Everything but the newest `keep` snapshots of main and the heads of branches and tags.

    That includes snapshots no ref reaches any more, such as those of a failed WAP batch whose
    branch was later replaced.
    """
    if keep < 1:
        raise ValueError("keep at least the current snapshot")
    kept = set(main_newest_first[:keep]) | protected
    return sorted(set(all_ids) - kept)


def unpublished(branch_newest_first: Iterable[int], main: set[int]) -> list[int]:
    """A branch's snapshots after the point where it leaves main's history.

    The stream sink's commits wait on its branch until the day is published (ADR 0021). They are
    not main's, so the newest-of-main rule would expire them and cut the branch off from main.
    """
    ahead = []
    for snapshot_id in branch_newest_first:
        if snapshot_id in main:
            break
        ahead.append(snapshot_id)
    return ahead


@dataclass(frozen=True)
class Expiry:
    expired: list[int]
    unreferenced_files: int
    unreferenced_bytes: int


def _all_data_files(table: Table) -> dict[str, int]:
    files = table.inspect.all_data_files().select(["file_path", "file_size_in_bytes"])
    return dict(
        zip(files["file_path"].to_pylist(), files["file_size_in_bytes"].to_pylist(), strict=True)
    )


def expire(table: Table, keep: int) -> Expiry:
    main = [s.snapshot_id for s in ancestors_of(table.current_snapshot(), table.metadata)]
    protected = {ref.snapshot_id for ref in table.refs().values()}
    for name, ref in table.refs().items():
        if name != "main" and ref.snapshot_ref_type == SnapshotRefType.BRANCH:
            head = table.snapshot_by_id(ref.snapshot_id)
            ancestry = (s.snapshot_id for s in ancestors_of(head, table.metadata))
            protected.update(unpublished(ancestry, set(main)))
    ids = snapshots_to_expire((s.snapshot_id for s in table.snapshots()), main, protected, keep)
    if not ids:
        return Expiry([], 0, 0)
    before = _all_data_files(table)
    table.maintenance.expire_snapshots().by_ids(ids).commit()
    after = _all_data_files(table.refresh())
    gone = set(before) - set(after)
    return Expiry(ids, len(gone), sum(before[path] for path in gone))


# --- reporting ---------------------------------------------------------------------------------


def history_rows(table: Table) -> pa.Table:
    """Main's snapshots, oldest first, with what each one did."""
    rows = []
    for s in main_ancestry(table):
        summary = s.summary.additional_properties if s.summary else {}
        rows.append(
            {
                "committed_at": datetime.fromtimestamp(s.timestamp_ms / 1000, UTC),
                "snapshot_id": s.snapshot_id,
                "operation": s.summary.operation.value if s.summary else None,
                "maintenance": summary.get(OPERATION_PROPERTY),
                "added_records": int(summary.get("added-records", 0)),
                "deleted_records": int(summary.get("deleted-records", 0)),
                "total_records": int(summary.get("total-records", 0)),
                "total_data_files": int(summary.get("total-data-files", 0)),
            }
        )
    return pa.Table.from_pylist(rows)


# --- CLI ---------------------------------------------------------------------------------------


def _show(con: duckdb.DuckDBPyConnection, title: str, sql: str) -> None:
    print(f"\n{title}\n{textwrap.dedent(sql).strip()}")
    con.sql(sql).show(max_width=200)


def _time_travel(table: Table, con: duckdb.DuckDBPyConnection, ident: str, day: date) -> None:
    snapshot_id = snapshot_as_of(table, day)
    if snapshot_id is None:
        raise SystemExit(
            f"{day}: its load is no longer in the history of main (expired?), "
            "so there is no state before later days to travel to"
        )
    # Rows that happened on `day`, by the day they arrived: later days bring its late events.
    query = """
        select cast(server_ts as date) as arrival_day, count(*) as events
        from lk.{ident}{at}
        where cast(event_ts as date) = '{day}'
        group by all order by arrival_day"""
    at = f" at (version => {snapshot_id})"
    _show(
        con,
        f"Before later days arrived (snapshot {snapshot_id}):",
        query.format(ident=ident, at=at, day=day),
    )
    _show(con, "Now:", query.format(ident=ident, at="", day=day))


def main() -> None:
    parser = argparse.ArgumentParser(description="Iceberg operations on the bronze table.")
    parser.add_argument("--namespace", default=NAMESPACE)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("snapshots", help="main's snapshots, oldest first, and the refs")
    commands.add_parser("files", help="live data files per arrival day and partition spec")
    p = commands.add_parser("partition", help="evolve the partition spec")
    p.add_argument("--grain", required=True, choices=sorted(GRAINS))
    p = commands.add_parser("time-travel", help="a day's events before and after late arrivals")
    p.add_argument("--day", required=True, type=date.fromisoformat)
    p = commands.add_parser("fragment", help="rewrite a day as many small files (demo aid)")
    p.add_argument("--day", required=True, type=date.fromisoformat)
    p.add_argument("--parts", type=int, default=24)
    p = commands.add_parser("compact", help="merge the files of fragmented days")
    p.add_argument("--day", type=date.fromisoformat, help="only this day (default: all fragmented)")
    p = commands.add_parser("expire", help="expire all but the newest snapshots of main")
    p.add_argument("--keep", type=int, default=5)
    args = parser.parse_args()

    env = os.environ
    ident = f"{args.namespace}.{TABLE_NAME}"
    table = lakehouse_catalog(env).load_table(ident)
    con = lakehouse_duckdb(env)

    if args.command == "snapshots":
        con.register("history", history_rows(table))
        _show(con, f"Main of {ident}, oldest first:", "select * from history")
        refs = ", ".join(f"{name} -> {ref.snapshot_id}" for name, ref in table.refs().items())
        print(f"refs: {refs}; {len(table.snapshots())} snapshots in the metadata")
    elif args.command == "files":
        con.register("files", pa.Table.from_pylist([vars(f) for f in data_files(table)]))
        _show(
            con,
            f"Live data files of {ident}; new writes use spec {table.spec().spec_id} "
            f"({partition_grain(table)}):",
            """
            select arrival_day, spec_id, count(*) as files, sum(records) as records,
                round(avg(size) / 1024) as avg_kib
            from files group by all order by all""",
        )
    elif args.command == "partition":
        changed = set_partition_grain(table, args.grain)
        table = table.refresh()
        state = "evolved to" if changed else "already"
        print(f"{ident}: {state} {args.grain} (spec {table.spec().spec_id}); old files keep theirs")
    elif args.command == "time-travel":
        _time_travel(table, con, ident, args.day)
    elif args.command == "fragment":
        files = fragment(table, args.day, args.parts)
        print(f"{args.day}: rewritten as {args.parts} appends, {files} data files")
    elif args.command == "compact":
        try:
            done = compact(table, [args.day] if args.day else None)
        except StreamPending as pending:
            print(f"compaction skipped: {pending}")
            return
        for c in done:
            print(f"{c.day}: {c.files_before} -> {c.files_after} data files")
        if not done:
            print("no fragmented days")
    elif args.command == "expire":
        result = expire(table, args.keep)
        print(
            f"expired {len(result.expired)} snapshots, kept the newest {args.keep} of main and "
            f"the ref heads; {result.unreferenced_files} data files "
            f"({result.unreferenced_bytes / 1024**2:.1f} MiB) are no longer referenced and stay "
            "on S3 (PyIceberg deletes no files)"
        )


if __name__ == "__main__":
    main()
