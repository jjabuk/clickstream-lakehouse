# ADR 0011: Two Postgres instances, one for the shop source and one for platform metadata

- **Status:** accepted
- **Date:** 2026-11-16

## Context

Postgres plays three different roles in the stack:

1. **Source system:** the `shop` database with orders. From week 5 Debezium reads its change
   stream through logical decoding.
2. **Catalog backend:** Lakekeeper stores its state (pointers to the current Iceberg metadata of
   every table) in Postgres.
3. **Airflow metadata database** (week 1, day 6).

The first version of the compose file ran all of them on one instance with several databases.

## Decision

Run two instances:

| Instance | Databases | Configuration |
|---|---|---|
| `postgres-shop` | `shop` | `wal_level=logical`, replication slots and WAL senders |
| `postgres-platform` | `lakekeeper`, later `airflow` | default configuration |

## Alternatives considered

- **One shared instance.** Simplest, and enough to make everything work. Rejected because
  `wal_level` and replication slots are instance-wide: a stalled Debezium connector leaves a slot
  that retains WAL until the volume fills up, and that takes down the catalog database with it.
  It also couples lifecycles: `shop` is wiped and regenerated often, the catalog must survive,
  because losing it orphans every Iceberg table in object storage.
- **Three instances (one each for shop, Lakekeeper and Airflow).** Isolation between the two
  metadata stores is not worth the extra container and memory on a laptop: both are low-traffic,
  default-configured and equally critical.

## Consequences

- One more container and a second host port (`5433` for the platform instance, `5432` for shop).
- Credentials are separate (`SHOP_POSTGRES_*`, `PLATFORM_POSTGRES_*` in `.env`), so a client cannot
  reach the catalog with the shop's credentials.
- `make reset`-style cleanups can later target the shop volume alone without touching the catalog.
- The split mirrors production practice: an operational source database and platform
  infrastructure are different systems, even when they run the same engine.
