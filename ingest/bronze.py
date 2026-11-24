"""Load a daily landing file into the Iceberg bronze table with PyIceberg.

Bronze keeps the data as it arrived: duplicates, late rows and skewed clocks are all still there
(cleaning is dbt's job). Two properties matter here:

- **Idempotent by partition.** The table is partitioned by `day(server_ts)`, and a batch is
  exactly one arrival day, so loading a day *replaces* that partition in one commit. A re-run
  gives the same table; it never doubles the rows.
- **Schema drift is absorbed.** A column that appears in the file but not in the table
  (`utm_campaign` from `utm_start`) is added with `union_by_name`; older rows read it as NULL.
- **Write-audit-publish** (ADR 0008). The day is written to a branch `wap_YYYYMMDD` made from the
  current `main`, audited there (`ingest.audit`), and `main` is fast-forwarded to it only if the
  audit passes. A failed batch stays on its branch; `main`, and everything dbt builds, never sees
  it. Schema changes are table-wide in Iceberg, so a new column reaches `main` before the audit.
"""

import warnings
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.json as pj
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import LongType, NestedField, StringType, TimestamptzType

from ingest.audit import HISTORY_DAYS, AuditResult, audit_batch

NAMESPACE = "bronze"
TABLE_NAME = "clickstream_events"

SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "event_type", StringType()),
    NestedField(3, "event_ts", TimestamptzType()),
    NestedField(4, "server_ts", TimestamptzType(), required=True),
    NestedField(5, "cookie_id", StringType()),
    NestedField(6, "user_id", LongType()),
    NestedField(7, "country", StringType()),
    NestedField(8, "device", StringType()),
    NestedField(9, "user_agent", StringType()),
    NestedField(10, "page_type", StringType()),
    NestedField(11, "page_url", StringType()),
    NestedField(12, "product_id", LongType()),
    NestedField(13, "quantity", LongType()),
    NestedField(14, "_source_file", StringType()),
    NestedField(15, "_ingested_at", TimestamptzType()),
)
PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=DayTransform(), name="server_ts_day")
)

# How the JSONL is parsed. Timestamps arrive as ISO-8601 strings and are cast afterwards;
# anything not listed here (a new column) is inferred and handled as schema drift.
TIMESTAMP_COLUMNS = ("event_ts", "server_ts")


def _parse_type(field: NestedField) -> pa.DataType:
    return pa.int64() if isinstance(field.field_type, LongType) else pa.string()


_PARSE_SCHEMA = pa.schema(
    [(f.name, _parse_type(f)) for f in SCHEMA.fields if not f.name.startswith("_")]
)


@dataclass(frozen=True)
class LoadResult:
    rows: int
    new_columns: tuple[str, ...]
    snapshot_id: int


class AuditFailed(Exception):
    """The batch broke the bronze contract; it is left on `branch` and `main` is unchanged."""

    def __init__(self, day: date, branch: str, result: AuditResult) -> None:
        self.day, self.branch, self.result = day, branch, result
        details = "; ".join(result.violations)
        super().__init__(f"{day}: audit failed, batch kept on branch '{branch}': {details}")


def wap_branch(day: date) -> str:
    return f"wap_{day:%Y%m%d}"


def lakehouse_catalog(env: Mapping[str, str]) -> Catalog:
    """The Lakekeeper catalog, with PyIceberg signing S3 requests with its own key (ADR 0003).

    The catalog vends no credentials (Garage has no STS) and does not sign requests, so the key
    comes from the environment, the same key DuckDB uses. The endpoint must resolve on the host
    too, which `garage` does through /etc/hosts (ADR 0012).
    """
    base = env.get("LAKEKEEPER_URL", "http://localhost:8181")
    return load_catalog(
        "lakekeeper",
        type="rest",
        uri=f"{base}/catalog",
        warehouse="lakehouse",
        **{
            "s3.endpoint": env["S3_ENDPOINT"],
            "s3.access-key-id": env["S3_ACCESS_KEY_ID"],
            "s3.secret-access-key": env["S3_SECRET_ACCESS_KEY"],
            "s3.region": env.get("S3_REGION", "garage"),
            "s3.path-style-access": "true",
        },
    )


def ensure_table(catalog: Catalog, namespace: str = NAMESPACE) -> Table:
    catalog.create_namespace_if_not_exists(namespace)
    table = catalog.create_table_if_not_exists(
        f"{namespace}.{TABLE_NAME}", schema=SCHEMA, partition_spec=PARTITION_SPEC
    )
    if table.current_snapshot() is None:
        # A branch starts from a snapshot of main, and a table without one cannot be written
        # to a branch at all. An empty commit gives the first audited day its base.
        table.append(table.schema().as_arrow().empty_table())
        table = table.refresh()
    return table


def arrival_day_filter(day: date):
    start = datetime.combine(day, time.min, tzinfo=UTC)
    return And(
        GreaterThanOrEqual("server_ts", start.isoformat()),
        LessThan("server_ts", (start + timedelta(days=1)).isoformat()),
    )


def _volume_history(table: Table, day: date) -> list[int]:
    """Rows per arrival day on main in the HISTORY_DAYS before `day`; days without data skipped."""
    start = datetime.combine(day - timedelta(days=HISTORY_DAYS), time.min, tzinfo=UTC)
    end = datetime.combine(day, time.min, tzinfo=UTC)
    ts = (
        table.scan(
            row_filter=And(
                GreaterThanOrEqual("server_ts", start.isoformat()),
                LessThan("server_ts", end.isoformat()),
            ),
            selected_fields=("server_ts",),
        )
        .to_arrow()["server_ts"]
        .to_pylist()
    )
    return list(Counter(t.date() for t in ts).values())


def publish(table: Table, branch: str, base_snapshot_id: int) -> Table:
    """Fast-forward main to the branch head, then drop the branch.

    Only a fast-forward: if main moved after the branch was made, publishing would silently drop
    those commits, so it refuses and the day has to be re-run on top of the new main.
    """
    table = table.refresh()
    head = table.current_snapshot()
    if head is None or head.snapshot_id != base_snapshot_id:
        raise RuntimeError(f"main moved since '{branch}' was created; re-run the day")
    table.manage_snapshots().set_current_snapshot(ref_name=branch).remove_branch(branch).commit()
    return table.refresh()


def read_landing(fs: pafs.S3FileSystem, path: str) -> pa.Table:
    with fs.open_input_stream(path) as f:
        raw = pj.read_json(
            f,
            parse_options=pj.ParseOptions(
                explicit_schema=_PARSE_SCHEMA, unexpected_field_behavior="infer"
            ),
        )
    for name in TIMESTAMP_COLUMNS:
        raw = raw.set_column(
            raw.schema.get_field_index(name), name, raw[name].cast(pa.timestamp("us", "UTC"))
        )
    n = raw.num_rows
    raw = raw.append_column("_source_file", pa.array([path] * n, pa.string()))
    return raw.append_column(
        "_ingested_at", pa.array([datetime.now(UTC)] * n, pa.timestamp("us", "UTC"))
    )


def conform(data: pa.Table, table: Table) -> pa.Table:
    """Arrange `data` in the table's column order, filling columns the file lacks with NULL."""
    target = table.schema().as_arrow()
    columns = [
        data[f.name].cast(f.type)
        if f.name in data.column_names
        else pa.nulls(data.num_rows, f.type)
        for f in target
    ]
    return pa.Table.from_arrays(columns, schema=target)


def evolve_schema(table: Table, data: pa.Table) -> tuple[Table, list[pa.Field]]:
    """Add the columns `data` has and the table lacks (schema drift, e.g. utm_campaign)."""
    known = set(table.schema().column_names)
    new_fields = [f for f in data.schema if f.name not in known]
    if new_fields:
        with table.update_schema() as update:
            update.union_by_name(pa.schema(new_fields))
        table = table.refresh()
    return table, new_fields


def load_day(
    catalog: Catalog,
    fs: pafs.S3FileSystem,
    path: str,
    day: date,
    namespace: str = NAMESPACE,
) -> LoadResult:
    table = ensure_table(catalog, namespace)
    data = read_landing(fs, path)

    table, new_fields = evolve_schema(table, data)

    start = datetime.combine(day, time.min, tzinfo=UTC)
    if data.num_rows and not (
        pc.min(data["server_ts"]).as_py() >= start
        and pc.max(data["server_ts"]).as_py() < start + timedelta(days=1)
    ):
        raise ValueError(f"{path} holds rows outside arrival day {day}")
    in_day = arrival_day_filter(day)

    # Write: to a fresh branch made from the current main. A branch left by an earlier failed
    # audit was made from an older main, so it is replaced, not reused.
    branch = wap_branch(day)
    base = table.current_snapshot().snapshot_id
    if branch in table.refs():
        table.manage_snapshots().remove_branch(branch).commit()
    table.manage_snapshots().create_branch(base, branch).commit()
    table = table.refresh()
    with warnings.catch_warnings():
        # The first load of a day deletes nothing, which PyIceberg reports as a warning.
        warnings.filterwarnings("ignore", message="Delete operation did not match any records")
        table.overwrite(conform(data, table), overwrite_filter=in_day, branch=branch)
    table = table.refresh()

    # Audit: what the branch now holds for the day, against the volume of the days before.
    head = table.refs()[branch].snapshot_id
    batch = table.scan(row_filter=in_day, snapshot_id=head).to_arrow()
    result = audit_batch(batch, _volume_history(table, day))
    if not result.passed:
        raise AuditFailed(day, branch, result)

    # Publish.
    table = publish(table, branch, base)
    return LoadResult(data.num_rows, tuple(f.name for f in new_fields), head)
