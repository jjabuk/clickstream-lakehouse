# ADR 0014: dbt reads users straight from Postgres until CDC arrives

- **Status:** accepted
- **Date:** 2026-11-18

## Context

`dim_user` is an SCD2 built with a dbt snapshot, and `dim_page` needs the product catalog.
Both live in the shop's Postgres (`users`, `products`, `categories`). Bronze holds only the
clickstream; Debezium puts Postgres tables into bronze as a change log in week 5. Until then
dbt needs another way to see them.

## Decision

dbt attaches the shop database read-only through DuckDB's `postgres` extension and declares the
tables as sources (`source('shop', 'users')`). Staging models over them are the only place that
knows where the data comes from, so in week 5 only those models change to read the CDC log.
The snapshot uses the `timestamp` strategy on `users.updated_at`, which the generator sets to
the simulated time of the change, so `dbt_valid_from` carries event time, not the wall clock.

## Alternatives considered

- **A temporary batch export of `users` to bronze through PyIceberg.** Keeps dbt reading only
  Iceberg, but it is code written to be deleted in week 5, with its own scheduling and
  idempotency to get right.
- **Debezium now.** Removes the problem, but pulls the whole of week 5 (Kafka Connect, Schema
  Registry) into week 2 and breaks the plan's order of risk.

## Consequences

- A dbt run needs the shop database up, which it is in the compose stack and will be in CI.
- Postgres keeps only the current row, so a snapshot sees the state *at the time dbt runs*.
  Several changes between two runs collapse into one, and a user changed twice shows only the
  last value. The generator applies one simulated day at a time, so running `dbt snapshot` after
  each `make day` keeps the history complete. The CDC log of week 5 removes this fragility.
- The attach is read-only, so dbt cannot change the source system by mistake.
