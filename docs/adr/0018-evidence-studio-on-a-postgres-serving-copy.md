# ADR 0018: Evidence (Studio CLI) reads a Postgres copy of the marts

- **Status:** accepted
- **Date:** 2026-11-23
- **Versions:** Evidence CLI 0.12.2 (single binary, checksum-pinned in `evidence/install_cli.sh`),
  DuckDB 2.0 `postgres` extension, Postgres 18.7.

## Context

The plan's week 3 ends with a dashboard in Evidence that reads the marts. The marts live in the
DuckDB file `data/warehouse.duckdb` (ADR 0013). Two facts decide how the dashboard can reach
them:

- **The npm Evidence (`@evidence-dev/*`, 40.x) is deprecated.** Its successor is the Evidence
  Studio CLI, a single binary with a new page syntax (Markdoc components such as
  `{% line_chart /%}`) and a semantic layer in `metrics/*.yaml`. The old DuckDB connector also
  bundles DuckDB 1.4, which cannot open a file written by DuckDB 2.0 (storage `v2.0.0+`).
- **The new CLI has no DuckDB file connector.** It queries a warehouse live through direct
  connectors (Postgres, Snowflake, BigQuery, ClickHouse and others) or, with an account, the hosted
  Evidence Warehouse.

A DuckDB file has one writer and no server, so even with a connector the dashboard would hold the
lock dbt needs, or read a file mid-build.

## Decision

**A serving copy in Postgres.** After every dbt build, `python -m serving` (`make marts-publish`,
and the `publish_marts` task at the end of the Airflow DAG) copies every `fct_*` and `dim_*`
table to the database `analytics` on postgres-platform:

1. DuckDB attaches the warehouse read-only and Postgres through its `postgres` extension, and
   writes the tables into a fresh schema `marts_new`.
2. One Postgres transaction drops `marts`, renames `marts_new` to `marts` and grants `SELECT` to
   the role `analytics_reader`. A reader sees the previous publication or the new one, never a
   mix.
3. `marts._published` records when it ran and the simulated clock (`SIMULATED_NOW`) the marts
   were built against. The dashboard's home page shows it.

The database and the role are created on first use. The Airflow task runs only after a passing
build, so a failed day never reaches the dashboard.

**The Evidence project** in `evidence/`:

- `connection.yaml` is committed. It names the read-only role, and the password is a
  `${ANALYTICS_READER_PASSWORD}` reference resolved from the environment.
- `metrics/funnel.yaml` and `metrics/traffic.yaml` define the metrics once. Every rate is a ratio
  of sums (`{checkout_sessions} / nullif({sessions}, 0)`), so it is correct at any grain and
  filter. An average of daily rates would weight a quiet day like a busy one.
- Four pages: overview, funnel (device and campaign filters), weekly retention cohorts, and
  traffic including bots.
- Pages do not use relative date presets such as `last 7 days`. They resolve against the wall
  clock, and the shop's days are simulated.
- The CLI runs with `DO_NOT_TRACK=1` and `EVIDENCE_TELEMETRY_DISABLED=1`, from the pinned binary
  in `~/.local/bin` (`make evidence-install`), not from the `curl | sh` installer.

## Alternatives considered

- **The npm Evidence with its DuckDB connector.** It reads the file directly, with no copy. But
  the package is deprecated, and its DuckDB 1.4 cannot read our file. Pinning DuckDB 1.x for the
  whole project to suit the dashboard would be backwards.
- **Evidence reading Iceberg.** Only bronze is in Iceberg (ADR 0013), and the dashboard needs the
  marts.
- **Lightdash.** It reads the dbt project's metrics, which is attractive. But it is a server with
  its own database and a dbt compile step, more services to run, and a local file shared with
  dbt has the same single-writer problem.
- **Metabase or Superset.** Click-built dashboards in an application database. That is the
  opposite of "analytics as code", which the plan asks for, and they are heavier to run.
- **Looker or Looker Studio.** Hosted and licensed, and they cannot reach a laptop's DuckDB file.
  LookML would be good interview material, but it is out of reach for a public local project.
- **Rill.** It reads DuckDB natively. But it embeds its own DuckDB, which has the same version
  coupling, and it is further from the plan than Evidence.
- **Postgres foreign tables or `pg_duckdb` over the file.** Live, with no copy. But the reader
  would hold the file open while dbt writes, which is what the copy avoids.

## Consequences

- **The dashboard lags the warehouse by one publish.** `make dbt-build` alone does not update it.
  Run `make marts-publish` after a manual build; the DAG does it on its own.
- **The copy is the full marts, every time.** That is about 80,000 rows, a second or two. At a
  larger scale it would become incremental per table, or the marts would move to a warehouse with
  a server (stage 2: Databricks SQL, which Evidence connects to directly).
- **Postgres is now a serving database as well as metadata storage** on postgres-platform
  (ADR 0011). It is a separate database with its own read-only role, so the dashboard cannot read
  Lakekeeper's or Airflow's tables.
- **`evidence validate` checks syntax and that the queries run.** It does not catch everything:
  a table `dimension` named after a metric-view dimension instead of a column passed validation
  and failed only in the browser. The pages were checked in `evidence dev`.
- **A task added to a DAG that has `depends_on_past` never starts in the first run after the
  change.** The previous run has no instance of the task. `publish_marts` sets
  `depends_on_past=False`; the dbt tasks before it already order the days.
- ADR 0013 expected Evidence to open the DuckDB file read-only. That consequence is superseded
  here.
