# ADR 0005: Kafka ingest without touching dbt, with a sink of our own

- **Status:** accepted
- **Date:** 2026-11-24
- **Versions:** Apache Kafka 4.4.0 (KRaft, one broker), Confluent Schema Registry 8.4.1,
  confluent-kafka 2.16.2, PyIceberg 0.12.0, Redpanda Console 3.15.0.

## Context

Until week 4 the clickstream reaches bronze as one JSONL landing file per arrival day, loaded by
PyIceberg (ADR 0003, 0008). A shop's clicks arrive as a stream, so week 4 replaces the file with
Kafka. The goal is that nothing downstream notices: the same rows land in the same bronze table,
and dbt and the marts stay unchanged.

The plan named the Kafka Connect sink of the Apache Iceberg project. It turned out to need too
much for an example project. Apache publishes it only as source: there is no runtime archive on
Maven Central or Confluent Hub, so it has to be built with Gradle from the release tarball. And
Iceberg 1.12.0 is built against Kafka 3.9; on Connect from Kafka 4 it fails to start a task
(`NoClassDefFoundError: AppInfoParser`), so Connect would also have to stay on the 3.9 line.

## Decision

**The generator produces Avro events to Kafka, and a small Python consumer writes them to bronze
with PyIceberg.** dbt, its sources and its models are not changed.

- **Producer.** `make stream-day` (and the DAG with `ingest=stream`) runs the generator with
  `--stream`. It sends every event of the day to the topic `clickstream.events` with the schema
  in the Schema Registry (ADR 0006). The same faults as in the files can be injected.
- **Partitions and key.** Three partitions, keyed by `cookie_id`. The plan said `user_id`, but most
  visitors are anonymous, so `user_id` is mostly null. dbt builds sessions per cookie, and one
  cookie in one partition keeps a visitor's events in order. Bots without a cookie have no key.
- **Sink.** The service `stream-sink` (`python -m ingest.stream sink`) reads the topic and appends
  batches of at most 2,000 rows to bronze's branch `stream` (ADR 0021). Each append stores the
  Kafka offsets it reached in its snapshot summary, in the same commit as the rows. Every batch
  starts at the offsets of the branch head, and a commit that finds the branch moved fails and
  is read again. So the branch is the consumer's checkpoint: no row is lost or written twice,
  with no consumer-group commits or Kafka transactions.
- **The same rows.** The sink fills `_source_file` with `kafka://clickstream.events/<partition>/<offset>`
  and `_ingested_at` with the commit time; everything else comes from the event. A new column
  (`utm_campaign`) evolves the table as the file load does.
- **Profile `stream`.** Kafka, the registry, the sink and Console run only with
  `make stream-up`, like Airflow. `make up`, the tests and CI still load files.
- **Proof.** `make ci-stream-replay` streams the CI days into a warehouse of its own, and
  `make marts-compare` compares every mart with the one `make ci-replay` built from files, using
  `audit_helper.compare_relations`.

## Alternatives considered

- **The Apache Iceberg Kafka Connect sink.** The standard choice, with exactly-once commits and
  a coordinator, and it can commit to a branch (`iceberg.tables.default-commit-branch`). Rejected
  for this project: it needs a Gradle build from source (three minutes, in a multi-stage image)
  and a Connect image on Kafka 3.9. That build does not belong in an example project.
- **Tabular's sink on Confluent Hub.** It installs in one line, but the project is archived and
  replaced by the Apache one.
- **Assembling the Apache sink from its Maven jars.** No compilation, but a Maven step that
  resolves about 250 jars, and still Connect on Kafka 3.9.
- **A stream processor (Flink, Spark Structured Streaming).** It would also handle lateness in
  the stream (ADR 0004), but it adds an engine for a job that is only "append to a table".

## Consequences

- The sink is about 100 lines of project code to maintain, instead of a configured connector.
  It has one task: it does not scale out over partitions, which 10,000 events a day do not need.
- Kafka Connect still arrives in week 5, for Debezium, whose plugin is a ready archive.
- Bronze's contract is unchanged, so dbt is too. Late events reach Kafka on their arrival day,
  exactly as in the files, and dbt's lookback window still handles them (ADR 0004).
- The sink commits every few seconds, so a streamed day lands as several small files. The
  Airflow DAG `iceberg_maintenance` compacts them on a schedule (ADR 0016).
- Duplicate events (the generator's dirt) are still in bronze and removed by staging on
  `event_id`, as before. A duplicate *delivery* is stopped by the audit (ADR 0021).
