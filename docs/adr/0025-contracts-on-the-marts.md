# ADR 0025: Contracts on the marts, with versions and a CI check

- **Status:** accepted
- **Date:** 2026-11-24
- **Versions:** dbt 2.0.12, confluent-kafka 2.16.2, Schema Registry 8.4.1.

## Context

Every mart already had `contract: enforced: true`: dbt fails a build whose columns or types do
not match the YAML. That protects a build from itself, not a consumer from a change. Nothing said
who may build on which model, who reads the marts, or how a mart changes without breaking its
readers. The producer side had the same gap: the Schema Registry refuses an incompatible Avro
schema only when the first message of it is produced, in the middle of a day's delivery.

## Decision

- **Access and one owning group.** All models belong to the group `analytics`. Staging is
  `private`, intermediate `protected`, and the marts `public`: the interface others build on.
- **Consumers are exposures.** The serving copy in Postgres (`application`) and each Evidence
  page (`dashboard`) list the marts they read, so the lineage of a mart ends at its readers.
- **A breaking change is a new version.** Removing or renaming a column, or changing its type,
  in a public mart means a new model version. The previous one stays, with a `deprecation_date`,
  until its readers have moved. The first one: `fct_sessions` v2 renames `utm_campaign` to
  `campaign`, the name of the semantic layer's dimension. v1 keeps the old name until 2027-01-31
  and is built from v2, so the two cannot drift. dbt v2 points the relation `fct_sessions` at the
  latest version, so readers that do not care follow it.
- **CI enforces it.** dbt Core stops a breaking contract change when it compares state; dbt v2
  does not (a changed type is not even `state:modified`). `make ci-contracts`
  (`ci/check_contracts.py`) compares the pull request's manifest with the production manifest of
  `main`: a removed or retyped column of a contracted model, a dropped contract, or a removed
  model fails the pull request, unless a version of the same model keeps the old columns, or the
  removed version is past its deprecation date.
- **The producer's contract is tested before it is used.** `tests/integration/test_schema_contract.py`
  asks the registry whether the schemas the generator can send are compatible with the live
  subject, and that a breaking change (a required field, a changed type) is refused.

## Alternatives considered

- **Changing a mart in place and announcing it.** Simple, and every reader breaks on the same
  day. Versions give the readers time, and the deprecation date says how much.
- **Relying on the contract and the tests alone.** They fail the build after the change, and only
  for the model itself. They do not say who loses a column, and a renamed column passes them.
- **A data contract tool outside dbt** (a separate YAML specification, a contract registry). One
  more definition of the same columns. The model YAML already is the contract, and the manifest
  carries it to CI.
- **Checking the producer's schema in the generator's unit tests.** Compatibility depends on what
  the registry already has, so it is asked there.

## Consequences

- A pull request that renames a mart column fails CI until it adds a version. The check runs only
  for pull requests with a production manifest from `main`; the first one has nothing to compare.
- Two tables per versioned model until the old version goes, and the serving copy carries both
  (`fct_sessions_v1`, `fct_sessions_v2`).
- The producer check needs the registry, so it runs with the stream tests (`make test-stream`),
  not in GitHub Actions, which has no stream profile.
