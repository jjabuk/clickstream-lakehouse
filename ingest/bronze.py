"""Load a daily landing file into the Iceberg bronze table with PyIceberg.

Bronze keeps the data as it arrived: duplicates, late rows and skewed clocks are all still there
(cleaning is dbt's job). Two properties matter here:

- **Idempotent by partition.** The table is partitioned by `day(server_ts)`, and a batch is
  exactly one arrival day, so loading a day *replaces* that partition in one commit. A re-run
  gives the same table; it never doubles the rows.
- **Schema drift is absorbed.** A column that appears in the file but not in the table
  (`utm_campaign` from `utm_start`) is added with `union_by_name`; older rows read it as NULL.
"""

import warnings
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


def lakehouse_catalog(env: Mapping[str, str]) -> Catalog:
    base = env.get("LAKEKEEPER_URL", "http://localhost:8181")
    return load_catalog("lakekeeper", type="rest", uri=f"{base}/catalog", warehouse="lakehouse")


def ensure_table(catalog: Catalog, namespace: str = NAMESPACE) -> Table:
    catalog.create_namespace_if_not_exists(namespace)
    return catalog.create_table_if_not_exists(
        f"{namespace}.{TABLE_NAME}", schema=SCHEMA, partition_spec=PARTITION_SPEC
    )


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


def _conform(data: pa.Table, table: Table) -> pa.Table:
    """Arrange `data` in the table's column order, filling columns the file lacks with NULL."""
    target = table.schema().as_arrow()
    columns = [
        data[f.name].cast(f.type)
        if f.name in data.column_names
        else pa.nulls(data.num_rows, f.type)
        for f in target
    ]
    return pa.Table.from_arrays(columns, schema=target)


def load_day(
    catalog: Catalog,
    fs: pafs.S3FileSystem,
    path: str,
    day: date,
    namespace: str = NAMESPACE,
) -> LoadResult:
    table = ensure_table(catalog, namespace)
    data = read_landing(fs, path)

    known = set(table.schema().column_names)
    new_fields = [f for f in data.schema if f.name not in known]
    if new_fields:
        with table.update_schema() as update:
            update.union_by_name(pa.schema(new_fields))
        table = catalog.load_table(f"{namespace}.{TABLE_NAME}")

    start = datetime.combine(day, time.min, tzinfo=UTC)
    in_day = And(
        GreaterThanOrEqual("server_ts", start.isoformat()),
        LessThan("server_ts", (start + timedelta(days=1)).isoformat()),
    )
    if data.num_rows and not (
        pc.min(data["server_ts"]).as_py() >= start
        and pc.max(data["server_ts"]).as_py() < start + timedelta(days=1)
    ):
        raise ValueError(f"{path} holds rows outside arrival day {day}")

    with warnings.catch_warnings():
        # The first load of a day deletes nothing, which PyIceberg reports as a warning.
        warnings.filterwarnings("ignore", message="Delete operation did not match any records")
        table.overwrite(_conform(data, table), overwrite_filter=in_day)
    return LoadResult(data.num_rows, tuple(f.name for f in new_fields))
