"""The producer's contract (ADR 0006, 0025): the generator's Avro schema evolves only compatibly.

The Schema Registry enforces BACKWARD compatibility when a schema is registered, which is when the
first message of a new schema is produced. These tests ask it before that: the schemas the
generator can send are compatible with the live subject and with each other in the order they
appear, and a breaking change is refused. Each check on its own subject, except the read-only
one against the live subject.
"""

import contextlib
import json
import uuid

import pytest
from confluent_kafka.schema_registry import (
    ConfigCompatibilityLevel,
    Schema,
    SchemaRegistryClient,
    ServerConfig,
)
from confluent_kafka.schema_registry.error import SchemaRegistryError

from generator.stream import TOPIC, avro_schema

pytestmark = [pytest.mark.integration, pytest.mark.stream]


@pytest.fixture
def registry(env):
    return SchemaRegistryClient({"url": env.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")})


@pytest.fixture
def subject(registry):
    """A subject of its own, BACKWARD like the live one, starting from the oldest schema."""
    name = f"test_{uuid.uuid4().hex[:8]}-value"
    registry.set_config(
        subject_name=name, config=ServerConfig(compatibility=ConfigCompatibilityLevel.BACKWARD)
    )
    registry.register_schema(name, avro(avro_schema(with_utm=False)))
    yield name
    for permanent in (False, True):
        with contextlib.suppress(SchemaRegistryError):
            registry.delete_subject(name, permanent=permanent)


def avro(schema: dict) -> Schema:
    return Schema(json.dumps(schema), "AVRO")


def test_the_generators_schemas_are_compatible_with_the_live_subject(registry):
    live = f"{TOPIC}-value"
    if live not in registry.get_subjects():
        pytest.skip(f"{live} is not registered yet: the first day produced registers it")
    for with_utm in (False, True):
        assert registry.test_compatibility(live, avro(avro_schema(with_utm))), with_utm


def test_utm_campaign_arrives_as_a_compatible_change(registry, subject):
    assert registry.test_compatibility(subject, avro(avro_schema(with_utm=True)))


@pytest.mark.parametrize(
    "change",
    ["a new field without a default", "event_id becomes a number"],
)
def test_a_breaking_change_is_refused(registry, subject, change):
    schema = avro_schema(with_utm=False)
    if change == "a new field without a default":
        schema["fields"].append({"name": "session_hint", "type": "string"})
    else:
        schema["fields"][0] = {"name": "event_id", "type": "long"}
    assert not registry.test_compatibility(subject, avro(schema))
