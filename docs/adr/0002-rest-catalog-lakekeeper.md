# ADR 0002: Iceberg REST catalog, implemented by Lakekeeper

- **Status:** accepted
- **Date:** 2026-11-16

## Context

An Iceberg table is a pointer to a metadata file, and the catalog owns that pointer. The choice
of catalog decides which engines can see the same tables and how commits are made atomic.

Requirements:

- PyIceberg, DuckDB and (in stage 2) Spark attach to the same catalog,
- runs locally in `docker compose` with a small footprint,
- speaks the standard Iceberg REST catalog protocol, so swapping the implementation does not
  touch ingest, dbt or the README examples.

## Decision

Use the Iceberg REST catalog protocol, served by **Lakekeeper** (verified latest release at the
time of writing: v0.15.1, 2026-11-15; the compose file pins the exact image tag). Lakekeeper keeps
its metadata in its own Postgres database, separate from the `shop` database that holds orders.

## Alternatives considered

- **Project Nessie.** Git-like semantics at the catalog level (branches across many tables).
  Rejected: WAP in this project works per table through Iceberg branches, so catalog-level
  branching would be a second, overlapping concept to explain.
- **Apache Polaris.** Open source REST catalog under the Apache umbrella, with a broader feature
  set (RBAC, federation). Rejected for stage 1 as heavier than needed for a single-user local
  stack. It remains a valid drop-in, since the client side is identical.
- **PyIceberg SQL/JDBC catalog.** The simplest option: a Postgres or SQLite table of pointers.
  Rejected because it is a client-side convention rather than a service, so DuckDB and Spark
  could not use it the same way, and the REST protocol is the interface stage 2 expects.
- **Hive Metastore.** Heavy JVM stack and a legacy interface; no upside here.
- **AWS Glue.** Out of scope: the project runs locally without AWS.

## Consequences

- One more service in compose (plus its Postgres database, a migration step and a bootstrapped
  warehouse), and a smoke test that proves PyIceberg and DuckDB both reach it.
- Lakekeeper can hand clients storage access in two ways: vended credentials (needs STS) or
  remote signing. The object store in use (ADR 0010) has no STS, so the warehouse is configured
  with remote signing for PyIceberg, and DuckDB, which does not implement remote signing, gets
  its own S3 credentials (`client-managed`). The day 2 smoke test must prove both paths.
- Because the contract is the REST spec, moving to Polaris or Nessie later is a configuration
  change, not a rewrite.
- Object storage backing the warehouse is a separate decision (see the compose setup).
