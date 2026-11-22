# ADR 0015: CI runs the real stack, with slim builds deferred to main and an incremental replay

- **Status:** accepted
- **Date:** 2026-11-22
- **Versions:** GitHub Actions on `ubuntu-24.04`, uv 0.13.7, dbt-core 1.12.7, dbt-duckdb 1.11.1.

## Context

`dbt build` needs data to mean anything: staging views read bronze through the Iceberg REST
catalog (ADR 0003), and users and products come straight from the shop Postgres (ADR 0014). There
is no shared production warehouse either. The marts live in a local DuckDB file (ADR 0013).

Two things have to be proven on every change:

1. the project builds and its tests pass on realistic data;
2. the incremental `fct_page_views` gives the same numbers as a full rebuild after late events
   have arrived over several runs (ADR 0004 names the full refresh as the reference).

The plan also asks for slim CI (`state:modified+`, `--defer`), which normally relies on a
production environment to defer to.

## Decision

The workflow `.github/workflows/ci.yml` has two jobs. Each step is a `make` target, so a run can
be reproduced locally.

- **`check`** needs no stack: ruff, sqlfluff, unit tests and `dbt_project_evaluator`. The
  evaluator reads only the project graph, so it runs on the in-memory `lint` target.
- **`dbt`** starts the real stack in docker compose (`make bootstrap`), runs the integration tests
  and then does one of two things.
  - **Full replay** (`make ci-replay ci-verify-incremental`) loads four arrival days around the
    `utm_campaign` schema change with a fixed seed. It runs `dbt build` after each day, so late
    events reach the incremental model over several runs, as in production. It then
    full-refreshes the incremental models and requires every table to be identical (multiset
    `EXCEPT ALL` both ways). Unit tests run once at the end, because those of an incremental
    model read the fixture's columns from the existing table.
  - **Slim build** (`make ci-days ci-slim`) loads the same days and builds only
    `state:modified+`, deferring every other ref. The ref points to the **production state**:
    `manifest.json` and `warehouse.duckdb` from the last green run on `main`, uploaded as an
    artifact. The `ci` dbt target attaches that file read-only under its database name
    (`warehouse`), so deferred relations resolve in the same DuckDB session.

`main` always runs the full replay and publishes the production state. A pull request runs the
slim build unless `ci/choose_mode.sh` finds a reason to run the full replay:

- there is no production state (first run, or the artifact expired after 30 days);
- the PR touches inputs of the data or pinned versions (`generator`, `ingest`, `infra`, `ci`,
  `Makefile`, `uv.lock`, `dbt/package-lock.yml`). Deferred tables would then come from different
  inputs than the rebuilt ones;
- an incremental model is in `state:modified+`. A slim build creates it from scratch and never
  exercises the incremental path, which is the part most likely to break.

## Alternatives considered

- **Unit tests and compile only, no stack.** Fast, but it misses everything that depends on real
  data: contracts against actual types, `relationships`, the snapshot, the lookback window. It
  would also have missed the two from-scratch bugs that the first run of this pipeline found
  (`utm_campaign` missing on days before it existed, and nondeterministic `any_value` in
  `fct_sessions`).
- **A cached production warehouse instead of a replay on `main`.** It would save about a minute,
  but its contents would depend on CI history rather than on the commit. The replay is
  deterministic: the same seed and days always give the same warehouse.
- **Slim build on every PR, without the full-mode rules.** It is the textbook setup, but it
  would let a change to the incremental logic merge without ever running incrementally.
- **dbt Cloud or a hosted warehouse for deferral.** That is out of scope for a local-only stage 1
  (no cloud accounts). Stage 2 on Databricks can defer to a real production schema.

## Consequences

- A PR typically finishes in a few minutes. Most of that is pulling images and starting the stack;
  the replay itself is about 30 s for four days.
- At this data size slim and full cost about the same. The slim path shows the mechanism and pays
  off as the project grows (marts in week 3, CDC in week 5).
- The production state is only as fresh as the last green `main` run. A PR is compared with the
  state of `main` at that run, not with the PR's merge base.
- `make bootstrap` followed by `make ci-replay` is a from-scratch build that anyone can run
  locally. It has to run on an empty stack, for example with
  `COMPOSE_PROJECT_NAME=clickstream-ci` and the dev stack stopped. A replay over a bronze that
  already holds every day is not incremental and can report false differences.
- Dimension changes are bound to facts at build time. The replay stays equal to a full refresh
  only because each change takes effect inside the lookback window of the run that first sees
  it, which holds as long as runs happen at least once per `lookback_hours`.
