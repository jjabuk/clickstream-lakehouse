# ADR 0003: PyIceberg writes, DuckDB reads

- **Status:** accepted, amended 2026-11-22 (PyIceberg signs with its own key, see below)
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
- **Both engines sign with their own S3 key** (amendment of 2026-11-22). The warehouse has remote
  signing switched off, and PyIceberg gets the key from the environment
  (`ingest.bronze.lakehouse_catalog`), like DuckDB.
- DuckDB writes stay possible for experiments, but no pipeline step depends on them.

## Amendment 2026-11-22: remote signing dropped for PyIceberg

The first CI runs on GitHub (ADR 0015) failed intermittently. Garage rejected PyIceberg writes as
anonymous (`Garage does not support anonymous access yet`, `403 Forbidden` on `HeadObject`), each
time on a different operation. Locally it never happened. PyIceberg's fsspec FileIO sends unsigned
requests (`signature_version=UNSIGNED`) and adds the signature from Lakekeeper in a `before-sign`
hook. That hook is attached to the botocore client of an s3fs instance, and s3fs can replace that
client (`set_session` with a refresh). The most likely explanation is that a request then went out
from a client without the hook. This could not be reproduced locally, so it is an explanation, not
a proof.

Lakekeeper returns the signer configuration whenever the warehouse has remote signing enabled,
whatever `X-Iceberg-Access-Delegation` the client asks for. A client-side override is therefore
not enough: the warehouse is configured with `remote-signing-enabled: false`
(`make catalog-init` also updates an existing warehouse), and PyIceberg receives `s3.endpoint`,
`s3.access-key-id` and `s3.secret-access-key` from `.env`.

**Alternative rejected:** keep remote signing and subclass the FileIO so that it re-attaches the
hook to every new client. It would keep the design, but it is a patch on internals of two
libraries (PyIceberg and s3fs) that an upgrade can silently break.

**Cost:** the catalog no longer mediates storage access, so every client holds a long-lived key.
That was already true for DuckDB. On a cloud store, vended credentials (STS) are the real answer,
and stage 2 on Databricks gets them from Unity Catalog.

## Alternatives considered

- **DuckDB as the writer too.** Simplest single-engine story, and the write support exists now.
  Rejected because the project's Iceberg operations (branches, snapshot expiration, compaction,
  partition evolution) are driven from PyIceberg, so one writer keeps commit semantics in one
  place; DuckDB's branch writes were not verified.
- **Spark for writes.** Full-featured but a heavy JVM dependency for a local stack; it enters in
  stage 2 on Databricks.
- **PyIceberg for reads as well.** Works, but dbt needs SQL; DuckDB gives it directly.

## Consequences

- DuckDB and PyIceberg hold a long-lived S3 key outside the catalog's access control:
  acceptable locally, but a real deployment would use vended credentials from the cloud store.
- The DuckDB write capability moves between versions, so DuckDB and PyIceberg stay pinned and the
  smoke tests are the early warning when an upgrade changes behaviour.
- Reads see a table as of the snapshot DuckDB loads; the dbt incremental lookback (ADR 0004)
  handles late data, not the engine.
