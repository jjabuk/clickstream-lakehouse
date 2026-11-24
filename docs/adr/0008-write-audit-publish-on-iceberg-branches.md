# ADR 0008: Write-audit-publish on Iceberg branches instead of tests after the fact

- **Status:** accepted
- **Date:** 2026-11-22
- **Versions:** PyIceberg 0.12.0, Lakekeeper 0.15.1, DuckDB 2.0.0.

## Context

dbt tests run after a batch has reached bronze. By then `fct_page_views` may already have merged
it, and a broken delivery, such as a file cut short or a file loaded twice, has become part of the
marts. What is needed is a gate in front of the data that readers see, not an alarm after it.

Bronze deliberately keeps dirt: duplicate deliveries of an event, late rows, skewed clocks and
bots. The gate must therefore reject *broken* batches without rejecting normal dirt.

## Decision

Every arrival day goes through write-audit-publish (`ingest.bronze.load_day`):

1. **Write** the day to a branch `wap_YYYYMMDD` created explicitly from the current snapshot of
   `main`. A branch left behind by an earlier failed audit is dropped and recreated, because it
   was made from an older `main`.
2. **Audit** what the branch holds for that day (`ingest.audit.audit_batch`):
   - the batch is not empty;
   - no `(event_id, server_ts)` delivery repeats, which would mean the same file was loaded
     twice. The same `event_id` delivered again at a later `server_ts` is normal dirt;
   - every `event_type` is a known type;
   - every page view has a `page_url`;
   - the row count is at least 50% of the median of the previous days on `main`, once there
     are three or more of them.
3. **Publish** by fast-forwarding `main` to the branch head and dropping the branch. If `main`
   moved in the meantime, publishing refuses instead of discarding those commits.

A failed audit leaves the batch on its branch for inspection, exits with code 1, and does not
move `main`. DuckDB, and with it dbt, reads only `main`, so nothing downstream sees the batch.
Running the same day again with a correct delivery replaces the branch and publishes.

`make day DATE=... FAULT=truncated|duplicated|bad_event_type` delivers a broken file on purpose
(`generator.faults`). The integration test `tests/integration/test_wap.py` is the acceptance
test: a truncated batch stays on its branch, `main` does not change, and the fixed delivery is
published.

### Two PyIceberg behaviours found while building it

- Writing to a branch that does not exist yet creates an **orphan branch with no parent**. It
  holds only the new rows, and fast-forwarding `main` to it silently drops everything `main` had
  before. That is why the branch is always created explicitly from `main` first.
- A table without any snapshot cannot be written to a branch. `ensure_table` therefore makes an
  empty commit when it creates the table, which gives the first day a base.

## Alternatives considered

- **dbt tests on bronze after the load** (the status quo). They give a clear signal, but too late:
  the data is already visible. They stay as a second line of defence.
- **Audit the Arrow table in memory before writing.** It is simpler and needs no branches. But a
  failed batch would leave no trace in the lakehouse, and the audit would not see the batch the
  way readers will, after conforming to the table schema and the overwrite.
- **Statistical gates only** (elementary anomalies). Good at spotting drift, but noisy on a short
  history and probabilistic by nature. elementary was warn-only and complemented the audit's
  fixed rules until it was removed with the move to dbt v2 (ADR 0024).
- **Nessie-style catalog branches** (a branch over the whole catalog). This is the alternative to
  Lakekeeper in ADR 0002. Table-level Iceberg branches are enough for a single-table bronze.

## Consequences

- **A schema change reaches `main` before the audit.** Iceberg schemas are table-wide, not
  per-branch, so a new column such as `utm_campaign` is added even if its first batch fails. The
  column is nullable and harmless, but it is not gated.
- **The volume threshold is a judgement.** With the default generator the lowest day over 60 days
  is 0.80 of its 7-day median, so 0.5 leaves room for normal variation and still catches a
  partial upload (20% of a day). A very small world with many bots swings more, which is why the
  test configuration uses one bot session a day.
- **DuckDB 2.0 cannot read a table whose only snapshot is the empty bootstrap commit** (`Invalid
  Avro file block count or size`). If the very first day of a new table fails its audit, dbt
  fails loudly until a day is published. It never reads wrong data.
- **One writer per day.** Two loads of the same day at once would race on the branch. The plan
  has one orchestrator (Airflow, this week), so this is acceptable.
- **Week 4 (Kafka sink) will need its own WAP story.** The Iceberg sink commits continuously to
  `main`, so the audit moves to a branch-committing sink configuration or to a publish step
  downstream. *Done in ADR 0021: the sink commits to the branch `stream`, and the task that
  delivered a day audits it and fast-forwards `main`.*
