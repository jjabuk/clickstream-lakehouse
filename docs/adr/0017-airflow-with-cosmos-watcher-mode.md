# ADR 0017: Airflow 3 with Cosmos in watcher mode, replaying simulated days by backfill

- **Status:** accepted
- **Date:** 2026-11-23
- **Versions:** Apache Airflow 3.4.0, astronomer-cosmos 1.16.0, dbt-core 1.12.7, dbt-duckdb
  1.11.1, uv 0.13.7.

## Context

Until now a day was `make day DATE=...` followed by `make dbt-build`, run by hand. The plan puts
Airflow in compose this week, with its metadata on postgres-platform (ADR 0011) and dbt
rendered through Astronomer Cosmos. Dagster was considered and rejected: see the alternatives.

Three properties of this project shape the setup:

- **DuckDB has one writer per file.** Cosmos's default (`ExecutionMode.LOCAL`) runs one dbt
  process per model and test. Run in parallel, those processes would fight over the lock on
  `data/warehouse.duckdb`. Run serially, they would mean about 90 dbt starts a day.
- **Time is simulated.** The shop's days are January and February 2026, and the wall clock is
  November. A scheduled run for "yesterday" would generate today's traffic into the history.
- **Days depend on each other.** The audit's volume check reads the previous days on `main`
  (ADR 0008), and the incremental models expect the previous day in place (ADR 0004).

## Decision

**Services.** Airflow 3.4 runs with the LocalExecutor as four services: api-server, scheduler,
dag-processor and triggerer. They sit in the compose profile `airflow`, so `make up`, the tests
and CI run without them; `make airflow-up` starts them. The metadata database is `airflow` on
postgres-platform, created by a one-shot `airflow-db-init`. The UI is on port 8088, without a
login (`SimpleAuthManager`, every user an admin; local only).

**Image.** `infra/airflow/Dockerfile` keeps two Python environments: Airflow's own with Cosmos,
under Airflow's constraints file, and `/opt/project-venv` with the project's locked dependencies
from `uv.lock` (generator, ingest, dbt). The repository is mounted, not copied, so a code
change needs no rebuild. The two environments never pin each other: DuckDB, pyarrow and
psycopg differ between Airflow's constraints and the project's lock.

**DAG `clickstream_daily`:**

```
replays_only >> load_day >> dbt (Cosmos task group) >> publish_marts
```

- **`load_day`** is `make day` without make: generate, then write-audit-publish, then advance the
  simulated clock. A failed audit fails the task, and nothing downstream runs.
- **`elementary_tables`** was removed with elementary (ADR 0024).
- **`dbt`** uses `ExecutionMode.WATCHER`. A single producer task runs one `dbt build`, and a
  deferrable sensor per model and test follows that node's status from the dbt log. One process
  writes DuckDB, and the graph still shows which model failed. dbt runs as a subprocess from the
  project's venv (`InvocationMode.SUBPROCESS`).
- **Cosmos settings** are `copy_dbt_packages=True` and `install_dbt_deps=False`. Cosmos runs dbt
  in a temporary copy of the project. By default it links `dbt_packages` but leaves out
  `package-lock.yml`, and without the lockfile dbt rejects the transitive `dbt_date` package.
  `make airflow-up` installs the packages on the host.

**Simulated time.**

- The schedule is daily with classic data intervals (`CronDataIntervalTimetable`), so a run's
  logical date is the day it handles.
- Days are replayed with `make airflow-backfill FROM=... TO=...`, one run at a time
  (`max_active_runs=1`).
- A single day can be run by hand with `make airflow-day DAY=...`. That target triggers with the
  day as the logical date and also passes it as the `day` param.
- `depends_on_past=True`, so a failed day stops the days after it. The run that fixes it is
  ordered among the simulated days, not at the wall-clock time.
- `replays_only` skips everything else. Unpausing the DAG, which a backfill needs, also creates a
  scheduled run for the latest wall-clock day. A manual run that does not name its day gets the
  wall clock as its logical date. Both are skipped.

**A faulty delivery in Airflow.** `make airflow-day DAY=... FAULT=truncated` breaks only the first
try. The audit stops it and the batch stays on its branch. Clearing `load_day` is the source's
corrected redelivery: try 2 publishes, and dbt builds.

**Lakekeeper.** `LAKEKEEPER__BASE_URI` is no longer set. It was pinned to `http://localhost:8181`
for remote signing, which is gone (ADR 0003, amended). Without it, Lakekeeper answers each client
with the address the client used: `localhost:8181` on the host, `lakekeeper:8181` inside the
network. Before the change, the catalog config sent containers to `localhost`.

## Alternatives considered

- **Dagster.** Software-defined assets fit this project: bronze, the dbt models and later the
  Feast features as one asset graph, the arrival day as a partition, and the audit as a blocking
  asset check. Airflow was kept because of the plan's goal of interview preparation. It is in far
  more job postings, and backfills, `depends_on_past`, executors and deferrable operators are
  standard questions.
- **Cosmos `ExecutionMode.LOCAL` with a one-slot pool.** It is correct, but about 90 dbt starts a
  day, a few seconds each.
- **A single `BashOperator` with `dbt build`.** It is simple, but Airflow shows one task, so the
  graph does not show which model failed.
- **`schedule=None` with only manual runs.** It avoids wall-clock runs altogether. But Airflow 3
  backfills need a schedule, and the backfill is how history is replayed.
- **dbt in Airflow's own environment.** It means one environment and `InvocationMode.DBT_RUNNER`.
  But it would install the project's pins into Airflow's constrained environment, where DuckDB
  and pyarrow already differ.

## Consequences

- **A run takes about 45 seconds:** generate, audit, publish, and a full `dbt build` of the
  project. A three-day backfill took about 2.5 minutes.
- **Airflow and the host share the dev warehouse and the simulated clock.** `make dbt-build` while
  a run builds would hit DuckDB's lock. Use one or the other at a time.
- **The DAG must be unpaused for a backfill to start.** A paused DAG queues backfill runs without
  running them. `make airflow-backfill` and `make airflow-day` unpause it. `airflow dags unpause`
  needs `--yes` when it runs without a terminal; without it, it waits for an answer forever.
- **Two runs writing bronze at once are refused, not merged.** Before `replays_only` existed, a
  scheduled run raced a backfill. `publish` refused the second fast-forward (ADR 0008), so no
  data was lost and the day was re-run.
- **CI does not run Airflow.** `make airflow-check` reports DAG import errors on the running
  stack. A DAG test in CI would need the image, which is a candidate for later.

## Update (ADR 0024)

- dbt v2 runs in the same watcher mode. Cosmos 1.16 supports dbt 2 and runs it as a subprocess.
  It was verified on a full `clickstream_daily` run.
- The image installs libduckdb 2.0.0 (`infra/duckdb/install_libduckdb.sh`). Without it, dbt v2
  downloads DuckDB 1.5.4, which cannot open the warehouse the host writes.
