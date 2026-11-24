from datetime import UTC, datetime

import pytest
from fastavro import parse_schema

from generator.stream import avro_schema, to_record
from ingest.stream import check_not_ahead


def _fields(schema: dict) -> dict[str, dict]:
    return {f["name"]: f for f in schema["fields"]}


def test_schema_gains_utm_campaign_as_optional_field_with_default():
    before, after = avro_schema(with_utm=False), avro_schema(with_utm=True)
    parse_schema(before), parse_schema(after)
    added = set(_fields(after)) - set(_fields(before))
    assert added == {"utm_campaign"}
    # BACKWARD compatibility: a reader with the new schema fills the field in old records.
    assert _fields(after)["utm_campaign"] == {
        "name": "utm_campaign",
        "type": ["null", "string"],
        "default": None,
    }


def test_server_assigned_fields_are_required():
    fields = _fields(avro_schema(with_utm=False))
    assert fields["event_id"]["type"] == "string"
    assert "default" not in fields["server_ts"]
    assert all("default" in f for n, f in fields.items() if n not in {"event_id", "server_ts"})


def test_record_keeps_the_millisecond_precision_of_the_landing_files():
    ts = datetime(2026, 2, 1, 10, 0, 0, 123987, tzinfo=UTC)
    record = to_record({"event_id": "e", "event_ts": ts, "server_ts": ts, "page_url": "/"})
    assert record["server_ts"].microsecond == 123000
    assert record["event_ts"].microsecond == 123000
    assert record["page_url"] == "/"


def test_record_allows_a_missing_client_timestamp():
    ts = datetime(2026, 2, 1, tzinfo=UTC)
    assert to_record({"event_id": "e", "event_ts": None, "server_ts": ts})["event_ts"] is None


def test_branch_ahead_of_a_recreated_topic_is_an_error():
    check_not_ahead({0: 5, 1: 7}, {0: 5, 1: 9})
    check_not_ahead({}, {0: 3})
    with pytest.raises(RuntimeError, match="recreated"):
        check_not_ahead({0: 9692}, {0: 1398})
