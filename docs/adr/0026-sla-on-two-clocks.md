# ADR 0026: An SLA on two clocks, and the alerts in one table

- **Status:** accepted
- **Date:** 2026-11-24
- **Versions:** dbt 2.0.12, Airflow 3.4.0, Evidence 0.12.2.

## Context

Nothing said whether the marts were complete. The WAP audits stop a broken delivery, but then
the day is simply missing: the dashboard shows yesterday's numbers as if they were today's, and
only a failed task in the Airflow UI tells anyone. `dbt source freshness` cannot help, because it
measures from the wall clock and the shop's days are simulated: every source is months "stale"
(the evaluator rule `fct_sources_without_freshness` has an exception for exactly that reason).

Two questions are mixed up in "is the data late": is the **data** behind the business day it
should cover, and is the **pipeline** slower than it should be. They run on different clocks.

## Decision

- **Data freshness on the simulated clock, defined on the marts.** A generic dbt test,
  `fresh_as_of_simulated_now`, compares the latest value of a column with `as_of` (the simulated
  now: `--vars` or `SIMULATED_NOW`, which `make dbt-build` reads from the clock file). The
  thresholds sit in the model YAML next to the contract: `fct_page_views`, `fct_sessions` and
  `fct_traffic_daily` warn 6 hours behind (a partial day) and fail 18 hours behind (a missing
  day). A daily column counts as complete until the end of its day. Without a clock, the test
  passes.
- **The check runs after every replayed day, whatever happened before.** The DAG's last task,
  `freshness` (`trigger_rule: all_done`), runs those tests for the day's end and records each
  warning or failure as an alert. A rejected delivery that nobody redelivered therefore shows up
  as four stale marts, not only as a red task.
- **Pipeline duration on the wall clock.** A deadline alert (Airflow 3): a run of
  `clickstream_daily` still not done 4 minutes after it was queued raises an alert. A day takes
  about a minute or two.
- **One table of alerts.** `ops.pipeline_alerts` in the serving database: when, which simulated
  day, warn or error, kind (`freshness`, `task_failed`, `deadline_missed`), subject and detail.
  Writers: the freshness check, an `on_failure_callback` on every task, and the deadline
  callback. The callbacks run in Airflow's environment and hand the alert to `python -m
  serving.alerts` in the project's. The Evidence page Data health and `make alerts` read it.

## Alternatives considered

- **`dbt source freshness`.** Wall clock only; with simulated time it can only be switched off.
- **Freshness on the sources (bronze) instead of the marts.** The marts are what the dashboard
  promises; a source can be fresh while a failed build leaves the marts behind.
- **Freshness in Python, outside dbt.** One more definition of what "fresh" means. In the model
  YAML it is reviewed with the model, runs in every `dbt build`, and lives next to the contract.
- **SLA misses (`sla=` on tasks).** Removed in Airflow 3; deadline alerts replace them.
- **A chat webhook or e-mail.** In a portfolio project the mechanism matters and the channel is
  one line in `serving.alerts`. A table is also something the dashboard can show.

## Consequences

- `make airflow-day DAY=... FAULT=...` without a redelivery leaves a `task_failed` alert and one
  freshness alert per mart; the redelivery clears the condition, and the alerts stay as history.
- A deadline is set when a run is created. Clearing a task of a finished run does not set a new
  one, so a slow redelivery raises no deadline alert.
- Not checked with backfills: if Airflow queues the runs of a backfill long before they start,
  the waiting ones would miss the deadline. A deadline from the run's start would then fit better.
- `dbt build` runs the freshness tests too, against the clock file (CI and the stream replay have
  their own). The clock moves only when a day is loaded, so a build alone never finds a missing
  day; that is the `freshness` task's job, with the day the run was for.
