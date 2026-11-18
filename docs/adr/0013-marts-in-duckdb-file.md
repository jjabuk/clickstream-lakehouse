# ADR 0013: Staging, intermediate and marts live in a DuckDB file

- **Status:** accepted
- **Date:** 2026-11-18
- **Versions:** dbt-core 1.12.7, dbt-duckdb 1.11.1, DuckDB 2.0.0.

## Context

dbt needs somewhere to materialise models. Bronze is an Iceberg table written by PyIceberg
(ADR 0003), and DuckDB reads it through the catalog. ADR 0003 also states that no pipeline step
may depend on DuckDB's Iceberg writes: they work today, but branch writes were not verified and
the capability moves between releases. The marts need incremental `merge` with a lookback
window and a snapshot (SCD2) for `dim_user`, both of which dbt-duckdb does on its own tables.

## Decision

Staging (views), intermediate models, marts, snapshots and later the feature tables are
materialised in one DuckDB database file, `data/warehouse.duckdb`. Bronze stays the only layer in
Iceberg. The Iceberg operations of week 3 (schema and partition evolution, time travel,
compaction, snapshot expiration) and write-audit-publish therefore act on bronze, which is where
a faulty batch enters; a bad batch stops on a branch of bronze and never reaches `main`, so
nothing downstream is rebuilt from it.

## Alternatives considered

- **Marts as Iceberg tables written by DuckDB.** One storage format end to end, and DuckDB can
  write. Rejected because ADR 0003 forbids depending on it, branch writes are unverified, and the
  incremental strategies dbt-duckdb offers target its own tables.
- **Marts written to Iceberg by PyIceberg after dbt.** Keeps Iceberg as the serving layer but
  splits every model across two tools and loses dbt's lineage and tests on the final table.
  Rejected as cost without benefit at this scale.

## Consequences

- A DuckDB file has a single writer: one dbt process at a time, and readers (Evidence, Feast)
  open it read-only or work on a copy. Fine locally, not a production design; on Databricks
  (stage 2) the same models land in Delta tables.
- Everything except snapshots is **rebuildable** from bronze with `--full-refresh`, so the file is
  disposable. `dim_user` is the exception: its history exists only in the snapshot table, because
  Postgres keeps just the current state (ADR 0014). Losing the file loses that history until CDC
  (week 5) puts a change log in bronze.
- Evidence and Feast read marts from a file path, not from the Iceberg catalog.
