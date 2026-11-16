"""Smoke tests for the local stack: storage, catalog, both engines and the source database."""

import psycopg
import pyarrow as pa
import pytest

pytestmark = pytest.mark.integration

SCHEMA = pa.schema([("id", pa.int64())])


def rows(*ids: int) -> pa.Table:
    return pa.table({"id": pa.array(ids, pa.int64())}, schema=SCHEMA)


def test_pyiceberg_roundtrip_through_remote_signing(catalog, namespace):
    table = catalog.create_table(f"{namespace}.t", schema=SCHEMA)
    table.append(rows(1, 2, 3))
    assert sorted(table.scan().to_arrow()["id"].to_pylist()) == [1, 2, 3]


def test_duckdb_reads_table_written_by_pyiceberg(catalog, namespace, duck):
    catalog.create_table(f"{namespace}.t", schema=SCHEMA).append(rows(1, 2, 3))
    assert duck.execute(f"SELECT count(*), sum(id) FROM lk.{namespace}.t").fetchone() == (3, 6)


def test_duckdb_write_is_visible_to_pyiceberg(catalog, namespace, duck):
    """Not our write path (ADR 0003), but proves both engines share one catalog."""
    catalog.create_table(f"{namespace}.t", schema=SCHEMA).append(rows(1))
    duck.execute(f"INSERT INTO lk.{namespace}.t VALUES (2)")
    table = catalog.load_table(f"{namespace}.t")
    assert sorted(table.scan().to_arrow()["id"].to_pylist()) == [1, 2]


def test_pyiceberg_writes_to_a_branch_without_touching_main(catalog, namespace):
    """The premise of write-audit-publish (ADR 0008): a staged write stays off `main`."""
    table = catalog.create_table(f"{namespace}.t", schema=SCHEMA)
    table.append(rows(1, 2))
    table.manage_snapshots().create_branch(table.current_snapshot().snapshot_id, "audit").commit()

    table = catalog.load_table(f"{namespace}.t")
    table.append(rows(3), branch="audit")

    table = catalog.load_table(f"{namespace}.t")
    on_main = table.scan().to_arrow()["id"].to_pylist()
    on_branch = table.scan(snapshot_id=table.metadata.refs["audit"].snapshot_id).to_arrow()
    assert sorted(on_main) == [1, 2]
    assert sorted(on_branch["id"].to_pylist()) == [1, 2, 3]


def test_shop_postgres_has_logical_decoding(env):
    with psycopg.connect(
        host="localhost",
        port=5432,
        dbname="shop",
        user=env["SHOP_POSTGRES_USER"],
        password=env["SHOP_POSTGRES_PASSWORD"],
    ) as conn:
        assert conn.execute("SHOW wal_level").fetchone() == ("logical",)
