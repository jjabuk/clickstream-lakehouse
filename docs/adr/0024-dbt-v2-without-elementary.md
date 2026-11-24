# ADR 0024: dbt v2, without elementary

- **Status:** accepted; supersedes [ADR 0019](0019-stay-on-dbt-core-1-12-for-now.md)
- **Date:** 2026-11-24
- **Versions:** dbt 2.0.12 (`pip install dbt`, the default distribution), DuckDB 2.0.0 everywhere,
  dbt_project_evaluator 2.0.2, dbt_utils 1.4.1, dbt_expectations 0.10.10, Cosmos 1.16.0.

## Context

ADR 0019 kept the project on dbt-core 1.12 because elementary 0.26.1, still the latest release,
fails on dbt v2 with DuckDB: its hooks look for temporary tables that do not exist. dbt v2 is what
dbt Labs installs by default and recommends for new projects. It also brings a built-in SQL
linter, project checks and a faster engine. The project should run the current major version, and
elementary was the only blocker.

## Decision

**Move to dbt v2 and drop elementary, without a replacement.** This project tests a stack on an
example problem; it is not a production template. elementary did three warn-only things here:

| elementary | Without it |
|---|---|
| `volume_anomalies` on bronze and `fct_page_views` | The write-audit-publish audit (ADR 0008) already stops a day that is much smaller than the recent median, or delivered twice, before it reaches `main`. That is a hard gate, not a warning. |
| `freshness_anomalies` on `fct_page_views` | Not covered. Days are loaded by hand or by the DAG, so a stale day is visible where it is loaded. |
| `schema_changes` on bronze | Schema evolution is handled where it happens: staging supplies `utm_campaign` as null before the day it appears. |
| `edr report` | Test results are in `target/run_results.json` and in the build output. |

dbt no longer reads the simulated clock. It stays for `make marts-publish`, which records it next to
the marts.

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
- **MetricFlow's CLI goes.** v2's `dbt sl` needs a dbt platform account, and `mf` is built on
  dbt-core 1.x, which cannot share an environment with v2 (both install the Python module `dbt`).
  Keeping it would mean a second locked environment only to query metrics locally. The semantic
  models and metrics stay in the YAML (ADR 0023).
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
- **v2 for the build, 1.12 for elementary.** Two engines and two manifests in one project.
- **Our own generic tests in place of elementary**: a z-score of each day's volume against the
  previous 14, the newest row against the simulated clock, and an expected column list for bronze.
  They were written and worked, and then removed. They mostly repeated the WAP audit, and they
  added code that the stack test does not need.
- **`dbt-oss`, the Apache-2.0 distribution of v2.** It has no `dbt lint`. dbt Labs recommends
  `dbt`, which is free to use; its licence covers the binary, not this repository's code.
- **DuckDB 1.5.4 everywhere, the driver dbt ships.** dbt is tested with it. But Python, PyIceberg's
  reader and the extensions would drop to 1.5, and every Mac with Homebrew's duckdb would have to
  be kept from loading 2.0. One explicit library is simpler.

## Consequences

- There is no statistical signal on volume or freshness after a day is published, and no test
  report. The WAP audit is the only volume check.
- **The DuckDB library is part of the environment,** not only of the lockfile. A new machine needs
  Homebrew's duckdb 2.0.0 (macOS) or `make libduckdb-install` (Linux). Without it, dbt quietly uses
  1.5.4, and the warehouse is written in the older format. That format can no longer be opened
  where DuckDB 2.0 has written to it.
- Cosmos's dbt 2 support is marked experimental. It is verified on the `clickstream_daily` run
  described in ADR 0017.
- When elementary supports v2 on DuckDB, it can come back if its anomalies and report are wanted.
