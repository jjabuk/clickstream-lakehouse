# ADR 0019: Stay on dbt Core 1.12 until v2 runs elementary on DuckDB

- **Status:** accepted
- **Date:** 2026-11-23
- **Versions:** dbt-core 1.12.7 and dbt-duckdb 1.11.1 (in use); dbt 2.0.12 (`pip install dbt`,
  tried); dbt-autofix (latest); elementary 0.26.1.

## Context

dbt v2, the engine formerly called Fusion, is what dbt Labs now installs by default and
recommends for new projects. dbt v2 is a Rust binary with adapters built on ADBC drivers.
dbt Labs states that 1.x "remains fully supported", but `dbt_project_evaluator` 2.x already
requires v2. The question is whether this project should move now. The answer came from trying:
dbt 2.0.12 was run on a copy of the project against the running stack, with nothing changed in
the repository.

## What the trial showed

| Step | Result |
|---|---|
| `dbt deps` | The same pinned packages install. The hub suggests `dbt_project_evaluator` 2.0.2. |
| `dbt parse` | It fails on one key until `dbt-autofix deprecations` is run. Autofix moves `loaded_at_field` under `config` and sets the flag `require_generic_test_arguments_property`. After that, parse passes. |
| Bronze over Iceberg REST | A profile `attach` with `type: iceberg` is not supported: v2 drops its options and DuckDB falls back to OAuth2. The catalog has to move to `catalogs.yml` (`type: iceberg_rest` with `use_catalogs_v2: true`). With that, the source reads all 131,139 rows of bronze. |
| Postgres attach, S3 secret, extensions | They work as configured. |
| Project models and tests | They build and pass with elementary excluded: 107 tests passed. The marts are identical to the 1.12 build row for row, comparing every column except the SCD2 keys, which depend on snapshot history. |
| elementary 0.26.1 | **It fails.** Its artifact models (`dbt_models`, `dbt_tests` and others) look for temporary tables that do not exist. The `on-run-end` hook fails, and its anomaly tests on the source skip the whole graph. |
| Unit tests of incremental models | On an empty warehouse they fail, because the mocked `this` needs the table. dbt 1.x behaves the same, and `make ci-replay` already runs unit tests last. This is not a v2 difference. |

These were not tried:

- **The rest of the toolchain**, which is Python and tied to dbt-core 1.x: `sqlfluff-templater-dbt` (v2 replaces it with `dbt lint`), the elementary CLI `edr`, MetricFlow's `mf` (`dbt-metricflow` requires dbt-core below 1.13), and Cosmos's subprocess mode in Airflow.
- **Slim CI with `--defer`** against a v2 manifest.

## Decision

Stay on dbt-core 1.12.7 with dbt-duckdb, and keep the project ready for v2:

- New YAML uses the specs that both versions read: the semantic layer's latest spec (ADR 0023),
  and test arguments under `arguments:`.
- The two changes autofix made are harmless on 1.12 and are applied when the migration starts.
- **Move when** elementary's DuckDB support works on v2, or when elementary is replaced. The
  move will need: the Iceberg catalog in `catalogs.yml`, `dbt lint` instead of the sqlfluff
  templater, Cosmos checked against the binary, and `mf` replaced by v2's own semantic-layer
  commands.

## Alternatives considered

- **Move to v2 now and drop elementary.** v2 is faster and has column-level lineage and static
  analysis. But volume and freshness anomalies on the simulated clock are a week 3 deliverable,
  and dbt tests have no replacement for them.
- **Run v2 for the build and 1.x for elementary.** That means two engines and two manifests in
  one project. It costs more than it saves at this size.
- **SQLMesh.** It has virtual environments, column lineage, plans and audits. Its maker was
  acquired by Fivetran in 2025, which also announced a merger with dbt Labs; the state of that
  should be checked before an interview. It would replace the dbt project rather than upgrade it.
  Stage 2 needs that same dbt project on Databricks, so SQLMesh stays a topic for interviews.

## Consequences

- The project runs the version line that most production projects still run. The upgrade path is
  known and measured, which is a better interview answer than either "we use the latest" or
  "we never looked".
- The trial was run with `dbt` installed in a scratch environment. The repository's lockfile was
  not touched.
- This decision gets revisited when elementary ships v2 support for DuckDB, or at the start of
  stage 2: Databricks is one of the adapters v2 supports first.
