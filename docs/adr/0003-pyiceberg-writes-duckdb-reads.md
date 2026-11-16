# ADR 0003: PyIceberg writes, DuckDB reads

- **Status:** accepted
- **Date:** 2026-11-16
- **Versions:** PyIceberg 0.12.0, DuckDB 2.0.0 with the `iceberg` and `httpfs` core extensions
  (extension builds are tied to the DuckDB release), Lakekeeper 0.15.1.

## Context

Two engines share one Iceberg catalog: something has to write the bronze tables from Python, and
something has to read them for dbt and ad-hoc analysis. CLAUDE.md requires recording the current
state of DuckDB's Iceberg support before relying on it. The state, verified against the local
stack (`tests/integration/test_stack.py`):

- DuckDB 2.0 attaches the REST catalog and **can write**: `INSERT`, `UPDATE`, `DELETE` and
  `CREATE TABLE` all succeeded through Lakekeeper, and PyIceberg saw the results.
- DuckDB **cannot use remote signing**, and Garage has no STS (ADR 0010). With the default
  `ACCESS_DELEGATION_MODE vended_credentials` the read fails with `403 Forbidden`. It works with
  `ACCESS_DELEGATION_MODE 'none'` plus an S3 secret holding its own key.
- PyIceberg 0.12.0 writes through remote signing and can append to a **branch**; the branch stays
  invisible on `main` until it is published (test `branch_without_touching_main`).

## Decision

- **Writes go through PyIceberg**: ingest into bronze, and later the branch-based
  write-audit-publish flow (ADR 0008) and table maintenance.
- **Reads go through DuckDB** (`dbt-duckdb` and ad-hoc queries), attached with
  `ACCESS_DELEGATION_MODE 'none'` and its own S3 key.
- DuckDB writes stay possible for experiments, but no pipeline step depends on them.

## Alternatives considered

- **DuckDB as the writer too.** Simplest single-engine story, and the write support exists now.
  Rejected because the project's Iceberg operations (branches, snapshot expiration, compaction,
  partition evolution) are driven from PyIceberg, so one writer keeps commit semantics in one
  place; DuckDB's branch writes were not verified.
- **Spark for writes.** Full-featured but a heavy JVM dependency for a local stack; it enters in
  stage 2 on Databricks.
- **PyIceberg for reads as well.** Works, but dbt needs SQL; DuckDB gives it directly.

## Consequences

- DuckDB holds a long-lived S3 key outside the catalog's access control: acceptable locally, but a
  real deployment would use vended credentials from the cloud store.
- The DuckDB write capability moves between versions, so DuckDB and PyIceberg stay pinned and the
  smoke tests are the early warning when an upgrade changes behaviour.
- Reads see a table as of the snapshot DuckDB loads; the dbt incremental lookback (ADR 0004)
  handles late data, not the engine.
