# ADR 0006: Avro with the Schema Registry, BACKWARD compatibility

- **Status:** accepted
- **Date:** 2026-11-24
- **Versions:** Confluent Schema Registry 8.4.1, confluent-kafka 2.16.2.

## Context

The events on Kafka need a format and a contract. The generator's raw events change once on
purpose: `utm_campaign` appears from 2026-02-01 (schema drift). Bronze absorbs a new column
already (ADR 0008); the question is what the topic allows and who checks it.

## Decision

- **Avro, with the schema in the Schema Registry** under the subject `clickstream.events-value`
  (the default topic-name strategy). The producer registers it, the sink reads it by the id in
  each message.
- **Everything but `event_id` and `server_ts` is optional** (a union with null and a default).
  The events are raw, and bronze keeps them as they came; checking content is the audit's job.
- **Timestamps are `timestamp-millis`**, the precision of the landing files, so both paths carry
  the same instants.
- **Compatibility: BACKWARD** (the registry's default, set explicitly). A new schema must read
  data written with the previous one. The generator uses the schema without `utm_campaign` until
  the events carry it; the day they do, it registers version 2 with an optional `utm_campaign`
  whose default is null. That is a backward-compatible change, so the registry accepts it.
- `event_type` stays a string, not an Avro enum: an unknown type must reach the audit, which
  rejects the window, instead of failing in the producer.

## Alternatives considered

- **JSON without a registry.** What the files are. No contract: a renamed field would only show
  up in bronze.
- **FULL or FORWARD compatibility.** FORWARD would let old consumers read new data, but the sink
  reads with the writer's schema anyway. FULL adds nothing for an optional field with a default.
- **Protobuf.** Equally good with the registry; Avro is the usual choice with Kafka and Iceberg.

## Consequences

- Removing or renaming a required field, or adding one without a default, is refused at
  registration, before any event is sent.
- The registry's `_schemas` topic is the only copy of the schemas; it lives on the Kafka volume.
