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
- **A lost race must fail, never be replayed.** PyIceberg 0.12 retries a commit that lost a race
  by replaying it on the newer snapshot (`commit.retry.num-retries`, 4 by default). For the sink
  that writes a batch twice: the batch was read from the offsets of the old head. Bronze sets the
  property to 0, so the commit fails and the sink reads again from the new head. A new column
  refreshes the table before the append, so the sink also checks that the branch head is still
  the one the batch started from. The failure tests found both (below).

## Failure tests

`make test-failures` (`tests/integration/test_stream_failures.py`, on the stack with
`make stream-up`) breaks the stream at one point per test, each on its own topic and table, and
checks one invariant: every message of the topic is on the branch head exactly once (by partition
and offset), the head's offsets are the end of the topic, and main is an ancestor of the branch.

| Failure | Expected, and tested |
|---|---|
| the sink fails after reading, before its commit | nothing committed; the same messages are read again |
| the sink process is killed with SIGKILL at random moments (seeded) | the invariant holds after a last clean run |
| two sinks commit the same batch | the later commit fails and reads on from the other's |
| the same, while the batch adds a column | as above (this one wrote the batch twice before the head check) |
| the topic is deleted and recreated | the sink stops with an error and the branch does not move |
| expiry while a window waits | the commits stay and the day publishes (the regression of 2026-11-24) |
| compaction while the branch has unpublished rows | `StreamPending` |
| a sink commit between compaction's check and its commit | the compaction fails, the sink's rows stay (failed through the commit retry before it was turned off) |
| publishing a day twice | the second run publishes nothing and rejects nothing |
| a message that is not Avro | the sink stops on it every time and never skips it; no dead-letter topic, since one broken message in a clickstream is a producer bug to fix, not data to route around |

Not automated, because they stop the services that the dev stack shares: a broker restart (the
data survives in the Kafka volume) and a Schema Registry outage (producer and sink fail loudly).
The registry keeps its schemas in the topic `_schemas`, so a lost Kafka volume also loses the
schema ids that old messages carry: the dev registry was restored once by re-registering them
in `IMPORT` mode.
