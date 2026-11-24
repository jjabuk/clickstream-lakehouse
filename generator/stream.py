"""Deliver a day's events to Kafka instead of a landing file (ADR 0005).

Each event is one Avro record on the topic `clickstream.events`. Its schema lives in the Schema
Registry under the subject `clickstream.events-value` (ADR 0006). The schema has `utm_campaign`
only once the events carry it, so the day the column appears registers a second version. That
version adds an optional field with a default, which BACKWARD compatibility accepts.

The key is the cookie, not `user_id`. Most visitors are anonymous, so `user_id` is mostly null,
and a null key spreads the events of one visitor over all partitions. dbt builds sessions per
cookie, so one cookie in one partition keeps a visitor's events in order. Bots without a cookie
have a null key and go to any partition.
"""

import json
from collections.abc import Mapping
from datetime import datetime

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from generator.dirt import RawRow

TOPIC = "clickstream.events"

_TIMESTAMP = {"type": "long", "logicalType": "timestamp-millis"}
# (name, Avro type) in the order of the bronze table. Everything but the two ids the server always
# sets is optional: the events are raw, and bronze keeps them as they came.
_FIELDS = [
    ("event_id", "string"),
    ("event_type", ["null", "string"]),
    ("event_ts", ["null", _TIMESTAMP]),
    ("server_ts", _TIMESTAMP),
    ("cookie_id", ["null", "string"]),
    ("user_id", ["null", "long"]),
    ("country", ["null", "string"]),
    ("device", ["null", "string"]),
    ("user_agent", ["null", "string"]),
    ("page_type", ["null", "string"]),
    ("page_url", ["null", "string"]),
    ("product_id", ["null", "long"]),
    ("quantity", ["null", "long"]),
]
_UTM = ("utm_campaign", ["null", "string"])


def avro_schema(with_utm: bool) -> dict:
    fields = _FIELDS + ([_UTM] if with_utm else [])
    return {
        "type": "record",
        "name": "ClickstreamEvent",
        "namespace": "shop.clickstream",
        "fields": [
            {"name": n, "type": t, **({"default": None} if isinstance(t, list) else {})}
            for n, t in fields
        ],
    }


def _millis(ts: datetime | None) -> datetime | None:
    """The landing file keeps milliseconds; the stream must carry the same instant."""
    return ts if ts is None else ts.replace(microsecond=ts.microsecond // 1000 * 1000)


def to_record(row: RawRow) -> dict:
    record = dict(row)
    record["event_ts"] = _millis(record.get("event_ts"))
    record["server_ts"] = _millis(record["server_ts"])
    return record


def produce(rows: list[RawRow], env: Mapping[str, str], topic: str = TOPIC) -> int:
    """Send the rows in arrival order and wait until the broker has acknowledged all of them."""
    registry = SchemaRegistryClient(
        {"url": env.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")}
    )
    schema = avro_schema(with_utm=any("utm_campaign" in r for r in rows))
    value = AvroSerializer(registry, json.dumps(schema), conf={"auto.register.schemas": True})
    key = StringSerializer()
    producer = Producer(
        {
            "bootstrap.servers": env.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            # No duplicates from the producer's own retries: the duplicates in the data are the
            # generator's dirt, not the transport's.
            "enable.idempotence": True,
            "linger.ms": 50,
        }
    )
    errors: list[str] = []

    def on_delivery(err, _msg) -> None:
        if err is not None:
            errors.append(str(err))

    ctx = SerializationContext(topic, MessageField.VALUE)
    for row in rows:
        record = to_record(row)
        cookie = record.get("cookie_id")
        producer.produce(
            topic,
            key=key(cookie) if cookie else None,
            value=value(record, ctx),
            on_delivery=on_delivery,
        )
        producer.poll(0)
    remaining = producer.flush(60)
    if remaining or errors:
        raise RuntimeError(f"{remaining} events not delivered; errors: {errors[:3]}")
    return len(rows)
