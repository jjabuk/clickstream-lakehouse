# ADR 0001: Apache Iceberg instead of Delta Lake as the table format

- **Status:** accepted
- **Date:** 2026-11-16

## Context

The project needs an open table format for the bronze layer and everything built on it. The
requirements come straight from the plan:

- several engines over the same tables (PyIceberg writes, DuckDB reads, Spark in stage 2),
- table-level operations that are part of the story: schema evolution (`utm_campaign`),
  partition evolution (`day` → `hour`), time travel, compaction, snapshot expiration,
- write-audit-publish: write a batch to a branch, test it, fast-forward to `main`,
- everything running locally, without a vendor platform.

## Decision

Use Apache Iceberg for all lakehouse tables in stage 1.

## Alternatives considered

- **Delta Lake.** Mature and the native format on Databricks (stage 2). Rejected for stage 1
  because the local, multi-engine, catalog-centric setup is Iceberg's home ground. Delta has no
  equivalent of table branches and tags, which WAP relies on, and partitioning changes are
  handled by rewriting or by liquid clustering rather than by partition evolution as a
  metadata-only operation.
- **Apache Hudi.** Strong on upserts and CDC, but a smaller ecosystem for the engines used here
  and no clear advantage for an append-only bronze layer. CDC is stored as a change log
  (ADR 7), so upsert performance is not the deciding factor.

## Consequences

- Branches and tags enable WAP (ADR 8). Partition evolution and time travel are demonstrable
  with before/after queries in the README.
- Stage 2 runs on Databricks, where Delta is native. How to bridge the two (UniForm, managed
  Iceberg tables in Unity Catalog, or plain Delta with the same dbt models) is a separate stage 2
  ADR; this decision does not commit to Iceberg on Databricks.
- Table maintenance (compaction, snapshot expiration, orphan file removal) is our job and is
  part of the scope in week 3.
- Writer and reader support differs per engine and per version, so each dependency is pinned
  (see ADR 3 for the DuckDB write state).
