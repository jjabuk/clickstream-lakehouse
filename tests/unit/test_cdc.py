"""Debezium's change events as bronze rows (ingest.cdc, ADR 0007)."""

import json
from datetime import UTC, datetime

from ingest.cdc import ARROW_SCHEMA, to_row

NOW = datetime(2026, 11, 24, tzinfo=UTC)


def event(op, before, after, table="orders", lsn=100):
    return json.dumps(
        {
            "before": before,
            "after": after,
            "op": op,
            "ts_ms": 1_790_000_000_500,
            "source": {
                "schema": "public",
                "table": table,
                "lsn": lsn,
                "txId": 7,
                "ts_ms": 1_790_000_000_000,
            },
        }
    ).encode()


def key(**pk):
    # ByLogicalTableRouter adds the source table to the key of the shared topic.
    return json.dumps({**pk, "__dbz__physicalTableIdentifier": "shop.public.orders"}).encode()


def test_an_update_keeps_both_row_images_and_the_bare_primary_key():
    row = to_row(
        key(order_id=1),
        event("u", {"order_id": 1, "status": "paid"}, {"order_id": 1, "status": "shipped"}),
        offset=42,
        now=NOW,
    )
    assert row["op"] == "u"
    assert (row["schema_name"], row["table_name"]) == ("public", "orders")
    assert row["key"] == '{"order_id": 1}'
    assert json.loads(row["before"])["status"] == "paid"
    assert json.loads(row["after"])["status"] == "shipped"
    assert (row["lsn"], row["tx_id"], row["kafka_offset"]) == (100, 7, 42)
    assert row["source_ts"] == datetime.fromtimestamp(1_790_000_000, UTC)


def test_an_insert_has_no_before_and_a_delete_no_after():
    insert = to_row(key(order_id=1), event("c", None, {"order_id": 1}), 0, NOW)
    delete = to_row(key(order_id=1), event("d", {"order_id": 1}, None), 1, NOW)
    assert insert["before"] is None and insert["after"] is not None
    assert delete["after"] is None and delete["before"] is not None


def test_row_images_are_canonical_json_so_a_redelivery_is_identical():
    a = to_row(key(order_id=1), event("c", None, {"status": "created", "order_id": 1}), 5, NOW)
    b = to_row(key(order_id=1), event("c", None, {"order_id": 1, "status": "created"}), 9, NOW)
    assert a["after"] == b["after"] and a["key"] == b["key"]


def test_every_field_is_in_the_bronze_schema():
    row = to_row(key(order_id=1), event("c", None, {"order_id": 1}), 0, NOW)
    assert set(row) == set(ARROW_SCHEMA.names)
