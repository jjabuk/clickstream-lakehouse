# ADR 0021: Write-audit-publish for the stream

- **Status:** accepted
- **Date:** 2026-11-24
- **Versions:** PyIceberg 0.12.0, Lakekeeper 0.15.1.

## Context

The file load writes a day to a fresh branch, audits it, and fast-forwards `main` (ADR 0008).
A stream has no file: rows arrive continuously, and the sink commits every few seconds
(ADR 0005). Three questions: where the sink writes, when a portion of the stream is complete
enough to audit, and who publishes it.

## Decision

- **Where:** one long-lived branch `stream` of bronze. The sink never writes to `main`.
- **The window:** one arrival day, as with files. It is **closed** when the generator has
  delivered the day (it returns once the broker acknowledged every event) and the offsets at the
  head of `stream` equal the end of the topic. The sink stores its offsets in each snapshot, so
  this needs no clock and no guess about lag.
- **The audit:** everything on `stream` after `main` must belong to the day; then the day's rows
  on the branch go through the file contract (`ingest.audit`: no duplicate deliveries, known
  event types, page URLs, volume against the last 7 days of `main`).
- **Who publishes:** the task that delivered the day, `make stream-day` or the DAG's `load_day`
  (`python -m ingest.stream publish`). It fast-forwards `main` to the head of `stream`.
- **A rejected window** is tagged `rejected_YYYYMMDD` for inspection. In the same commit, `stream`
  is reset to `main` and given an empty commit with the topic's end offsets, so the sink skips the
  rejected messages. The corrected redelivery (a retry in Airflow) starts from a clean branch.
- **Compaction keeps `stream` a descendant of `main`.** It runs only while `stream` is at `main`,
  and moves it to the compacted head in the same commit (ADR 0016).

## Alternatives considered

- **A staging table instead of a branch.** The fallback in the plan, in case the sink could not
  commit to a branch. Publishing would copy rows instead of moving a ref, and a rejected portion
  would have to be deleted from the staging table.
- **A branch per window** (`wap_YYYYMMDD`, as for files). The sink would need to switch branches
  at midnight. A static branch keeps the sink unaware of windows.
- **Closing the window by consumer lag.** Lag zero says the consumer read the messages, not that
  they are committed to the table. The offsets in the snapshot say exactly that.
- **Closing by event time (a watermark).** Lateness up to 48 h would hold every window open for
  two days. The window is the arrival day, as in the files, so lateness stays dbt's job (ADR 0004).

## Consequences

- **One window at a time.** Publishing fast-forwards everything on the branch, so a day is
  delivered and published before the next one is sent. That is how `make stream-day` and the
  DAG run. A real always-on producer would need windows cut on the branch, or a staging table.
- **One ingest at a time.** A file load moves `main` past the stream branch, after which `stream`
  cannot be fast-forwarded. Switching back to files means dropping the `stream` branch first.
- A rejected portion stays readable through its tag until it is expired.
- **The topic and the branch belong together.** The branch records offsets into the topic, so a
  recreated topic (a deleted Kafka volume) no longer matches it. The sink and the publish step
  then stop with an error instead of skipping or re-reading data; the branch needs new offsets
  before the stream can go on.
- **Expiry keeps every commit waiting on `stream`.** Publishing needs main to be an ancestor of
  the branch head, and the sink finds its offsets by walking back from it. A first version kept
  only the newest snapshots of main and the ref heads: a scheduled `expire --keep 20` removed four
  of a day's five sink commits before publishing, and the fast-forward failed. Expiry now keeps a
  branch's snapshots after the point where it leaves main.
