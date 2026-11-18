# ADR 0004: Late data is handled by a lookback window in dbt

- **Status:** accepted
- **Date:** 2026-11-18

## Context

Events reach the warehouse up to 48 hours after they happened, and a client clock can be hours
off (generator dirt, see `generator/dirt.py`). A late event changes finished facts: it adds a page
view to an old day, and it can extend a session or bridge two sessions into one. The question is
where to absorb that: in the transformation layer on every batch, or earlier in a stream
processor with watermarks and allowed lateness (relevant from week 4, when Kafka replaces the
files).

## Decision

dbt absorbs it with a lookback window, set by `lookback_hours` (default 48):

- Bronze is **append-only by arrival day**: a late event lands in the partition of the day it
  arrived, so old partitions are never rewritten.
- `fct_page_views` is incremental, `merge` on `event_id`. Each run reprocesses every row that
  arrived since the last run or occurred within `lookback_hours` of the newest arrival, plus
  `session_context_hours` of older rows so the `LAG` that sessionizes has its neighbours.
  Rows older than the window are provably unaffected: a newly arrived event occurred no earlier
  than the window start, so it cannot precede the first event of an older session or sit between
  two older sessions.
- `fct_sessions` is a plain table aggregated from the fact, because a bridging event makes a
  session id disappear and a `merge` cannot delete the stale row.
- Staging keeps the first delivery of each `event_id`, so a duplicate that arrives hours later
  neither re-enters the window nor changes a number.

## Alternatives considered

- **Full refresh on every run.** Always correct and the simplest, but cost grows with history.
  Kept as the reference: the acceptance test compares the incremental result with it.
- **Handle lateness in the stream** (watermark, allowed lateness, retractions). Closer to the
  source and cheaper downstream, but it needs a stream processor (Flink or similar) that the plan
  keeps as vocabulary only, and the batch path would still need a correction step for anything
  beyond the allowed lateness.
- **Rewrite partitions by event time.** Natural for Hive-style layouts, but on Iceberg it turns
  every late event into a rewrite of an old partition and defeats the append-only bronze.

## Consequences

- An event later than `lookback_hours` is not reprocessed against its neighbours. The generator's
  bound is 48 h of lateness plus up to 2 h of clock skew, so the largest lag it can produce is
  50 h, slightly above the default; a test reports events whose lag exceeds the window.
- Every run recomputes about `lookback_hours` of data, a fixed cost per run.
- Sessions are assumed shorter than `session_context_hours` (12 h); a test guards it.
- In week 4 the same events arrive through Kafka. If the sink delivers them the same way, dbt
  stays unchanged, which is what the audit-helper comparison in that week verifies.
