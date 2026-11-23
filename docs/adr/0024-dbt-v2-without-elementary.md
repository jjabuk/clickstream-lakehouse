# ADR 0024: dbt v2, with our own anomaly tests instead of elementary

- **Status:** accepted; supersedes [ADR 0019](0019-stay-on-dbt-core-1-12-for-now.md)
- **Date:** 2026-11-24
- **Versions:** dbt 2.0.12 (`pip install dbt`, the default distribution), DuckDB 2.0.0 everywhere,
  dbt_project_evaluator 2.0.2, dbt_utils 1.4.1, dbt_expectations 0.10.10, Cosmos 1.16.0.
  dbt-metricflow 0.16.0 with dbt-core 1.12.7 in a separate environment, only for `mf`.

## Context

ADR 0019 kept the project on dbt-core 1.12 because elementary 0.26.1, still the latest release,
fails on dbt v2 with DuckDB: its hooks look for temporary tables that do not exist. dbt v2 is what
dbt Labs installs by default and recommends for new projects. It also brings a built-in SQL
linter, project checks and a faster engine. The project should run the current major version, and
elementary was the only blocker.

## Decision

**Move to dbt v2 and drop elementary.** elementary did three things here, and each is replaced:

| elementary | Replacement (`dbt/macros/tests/`) |
|---|---|
| `volume_anomalies` on bronze and `fct_page_views` | `volume_anomaly`: the row count of the last complete day before the simulated clock, as a z-score against the previous 14 days. It warns at \|z\| > 3 and needs 5 days of history. |
| `freshness_anomalies` on `fct_page_views` | `fresh_against_clock`: the newest `received_at` must be within 6 hours of the simulated clock. |
| `schema_changes` on bronze | `expected_columns`: bronze's columns are listed next to the source. `utm_campaign` is optional, because schema evolution adds it on 2026-02-01. |
| `edr report` | No replacement. Test results are in `target/run_results.json` and in the build output. |

All three are warn-only, like before. "Now" is the simulated clock (macro `simulated_now`, from
`SIMULATED_NOW`), because time in the data is simulated.

**What else changed with v2:**

- **Iceberg catalog.** v2 does not pass a DuckDB `attach` with `type: iceberg` through, so bronze's
  REST catalog is declared in `dbt/catalogs.yml` (flag `use_catalogs_v2`).
- **dbt_project_evaluator 2.x.** Its rules are now dbt checks. They run before every build and
  alone with `dbt check` (`make dbt-evaluate`), with no warehouse. Exceptions moved from a seed to
  the variable `dbt_project_evaluator_exceptions`. Violations are errors (`checks: +severity:
  error`), as they failed `make check` before.
- **`dbt lint` instead of sqlfluff.** It reads the same `.sqlfluff` (moved to `dbt/`) and the same
  rule codes, and it is much faster. It flagged five column references inside scalar subqueries
  that sqlfluff had accepted (RF02); they are now qualified. A rule without an autofix only warns
  by default, so `make lint-sql` turns every lint warning into an error.
- **MetricFlow.** v2's `dbt sl` needs a dbt platform account. `mf` stays, in its own environment
  (`tools/metricflow`, locked separately), because it is built on dbt-core 1.x and both packages
  install the Python module `dbt`. It reads the `semantic_manifest.json` that v2 writes.
- **Airflow.** Cosmos 1.16 supports dbt 2. Watcher mode runs dbt as a subprocess, as before. The
  `elementary_tables` task is gone.
- **Usage statistics are off** (`DBT_SEND_ANONYMOUS_USAGE_STATS=false`) in the Makefile and the
  Airflow containers.

**One DuckDB version: 2.0.0.** dbt v2 loads a system `libduckdb` when there is one, and otherwise
downloads its own driver, DuckDB 1.5.4 for dbt 2.0.12. On macOS it found Homebrew's 2.0.0. In the
Airflow container it downloaded 1.5.4, which cannot open a warehouse file that 2.0 wrote, and the
first Airflow run failed on exactly that. `infra/duckdb/install_libduckdb.sh` installs the official
2.0.0 library, with a pinned version and sha256, in the Airflow image and on the CI runner
(`make libduckdb-install`). DuckDB 2.0.0 is now the same in dbt, in Python (`duckdb==2.0.0`), and in
the CLI behind `make warehouse-shell`.

## Alternatives considered

- **Stay on 1.12 until elementary supports v2 (ADR 0019).** It is safe, but it leaves the
  project on the old major version for an unknown time, for one package.
- **v2 for the build, 1.12 for elementary.** Two engines and two manifests in one project. It costs
  more than three small tests.
- **`dbt-oss`, the Apache-2.0 distribution of v2.** It has no `dbt lint`. dbt Labs recommends
  `dbt`, which is free to use; its licence covers the binary, not this repository's code.
- **DuckDB 1.5.4 everywhere, the driver dbt ships.** dbt is tested with it. But Python, PyIceberg's
  reader and the extensions would drop to 1.5, and every Mac with Homebrew's duckdb would have to
  be kept from loading 2.0. One explicit library is simpler.
- **dbt_expectations for the anomalies.** It has no z-score test over days, and its time-based tests
  compare with the wall clock.

## Consequences

- Volume and freshness checks are simpler than elementary's. They have no seasonality and no
  stored history. With about 8,500 rows a day, a truncated day (about 1,900 rows) scores z ≈ −7,
  and a normal day scores about ±1.
- The anomaly tests have no report UI. That is acceptable at this size; Evidence or the dbt docs
  could show `run_results.json` later.
- **The DuckDB library is part of the environment,** not only of the lockfile. A new machine needs
  Homebrew's duckdb 2.0.0 (macOS) or `make libduckdb-install` (Linux). Without it, dbt quietly uses
  1.5.4, and the warehouse is written in the older format. That format can no longer be opened
  where DuckDB 2.0 has written to it.
- Cosmos's dbt 2 support is marked experimental. It is verified on the `clickstream_daily` run
  described in ADR 0017.
- When elementary supports v2 on DuckDB, it can come back for its report. The tests above can stay
  either way.
