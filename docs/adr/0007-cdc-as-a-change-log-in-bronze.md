# ADR 0007: The shop's orders through CDC, kept in bronze as a change log

- **Status:** accepted
- **Date:** 2026-11-25
- **Versions:** Debezium 3.7.1.Final (Kafka Connect, Postgres connector, pgoutput), Kafka 4.4.0,
  Postgres 18.7, PyIceberg 0.12.0, dbt 2.0.12.

## Context

The shop's orders live in Postgres, and Postgres keeps only the current row: an order that was
paid, shipped and delivered says `delivered`, with the time of the last change. A nightly copy
of the table would lose every milestone between two copies, and any analysis of the order
lifecycle (time to ship, returns after delivery, cancellations before payment) needs them.
The generator plays one `UPDATE` per status change, with the simulated time in `updated_at`, so
the source has the history; only its WAL keeps it.

## Decision

- **Debezium reads the WAL** (logical decoding, `pgoutput`) and writes every insert, update and
  delete of `orders`, `order_items` and `cdc_watermark` to Kafka. `orders` has `REPLICA IDENTITY
  FULL`, so an update carries the row before it too. The connector is registered from
  `infra/connect/shop-cdc.json` by `connect-init`, with the credentials from the container's
  environment, not in the file.
- **One topic, one partition, for all the shop's tables** (`shop.changes`, through the
  `ByLogicalTableRouter` transform). Kafka keeps order within a partition only, so this keeps the
  order in which Postgres committed the changes, across tables: an order's lines never arrive
  after a later change of the order.
- **Bronze keeps the log as it is** (`bronze.shop_changes`): one row per change, `op`, `before`
  and `after` as JSON text, the LSN, the commit time, the Kafka offset. The sink (`cdc-sink`,
  `ingest/cdc.py`) commits the Kafka offsets in the Iceberg snapshot with the rows, as the
  click stream's sink does (ADR 0021): a crash or a lost race never loses or repeats a batch.
  There is no write-audit-publish. A change log has no day to close, and a change is a fact
  about the source; deciding what it means is staging's job.
- **The end of a day is a watermark.** `python -m ingest.cdc wait` (the last step of `make day`,
  `make stream-day` and the DAG's `load_day`) inserts a row with a new id into `cdc_watermark`
  and waits until that row is in bronze. With one partition, every change committed before it
  is there too. This is the watermark pattern of Netflix's DBLog, which Debezium's incremental
  snapshots use as well.
- **dbt reads the log, and staging deduplicates.** Debezium delivers at least once: after a
  restart it can send changes again. A redelivered change has the same key, `op` and LSN, so
  `stg_shop_cdc__order_changes` keeps the first delivery. The business time of a change is the
  row's `updated_at` (simulated), not Debezium's timestamps (the wall clock when the generator
  ran).
- **`fct_orders` is an accumulating snapshot** (Kimball): one row per order, its current status
  and one timestamp per milestone, plus flags, revenue and durations. `fct_order_status_changes`
  is the transaction fact underneath, one row per status an order entered.
- **History starts when CDC started.** On its first start Debezium snapshots the rows already
  there (`op = 'r'`), with only their state at that moment. Those orders have
  `is_history_complete` false: their earlier milestones are unknown, and only durations need
  them. Counts and revenue come from the current status and stay complete.
- **Kafka and Connect move into the core stack.** Orders always arrive through CDC, so `make up`
  and CI start Kafka, Connect and the sink; the click stream's Schema Registry, sink and Console
  stay in the `stream` profile.

## Alternatives considered

- **Upsert in the sink (a current-state table in bronze).** Simpler to query, and exactly the
  loss this ADR is about: the history would be gone in bronze already. The current state is a
  `qualify row_number()` over the log in staging; the reverse is impossible.
- **A topic per table** (Debezium's default). Lets each table scale on its own, which a shop of
  this size does not need, and loses the order between tables. The watermark would then have to
  be read on every topic.
- **Query-based CDC** (`WHERE updated_at > last run`). Misses deletes and every intermediate
  state between two polls, and depends on the application setting `updated_at` correctly.
- **Debezium Server, or Flink CDC, writing straight to Iceberg.** One service fewer, but the
  change log would not be in Kafka for other consumers, and the exactly-once argument would rest
  on another project's sink instead of the one tested here (ADR 0021).
- **Avro with the Schema Registry for the change events.** The JSON converter without embedded
  schemas is enough for one consumer that stores `before` and `after` as text; Postgres's DDL is
  the schema, and staging types the columns. Avro is used where the producer is ours (ADR 0006).
- **Rebuilding the missing milestones of old orders from the generator's timeline.** The
  warehouse would then know what the source never recorded; it is honest to say where history
  starts instead.

## Consequences

- `make day` takes a few seconds more: the watermark's round trip through Debezium and the sink
  (about 3 to 6 s).
- Debezium needs a replication slot. A slot that nobody reads keeps WAL on the shop's disk;
  `make cdc-status` shows the connector and the slot's lag.
- The publication is `FOR ALL TABLES` and the connector captures the three tables in every
  schema, so a table created later (the first `make day` on a fresh stack, a test's schema) is
  captured without a restart. Tests write to a schema of their own and delete their rows from
  bronze afterwards; staging reads `public` only. Every table in the publication needs a
  primary key or a replica identity, which all of the shop's have.
- The source table `bronze.shop_changes` feeds two staging models, one per shop table in it; the
  evaluator's `fct_source_fanout` rule has an exception for it.
- Users and the product catalog are still read from Postgres directly (ADR 0014): `dim_user`'s
  snapshot is complete because the generator applies one day at a time. Moving them to the log
  would replace the dbt snapshot with an SCD2 built from the changes; it is not needed for this
  week's goals.
- The freshness SLA (ADR 0026) covers the orders too, on `fct_order_status_changes.changed_at`.
- The sink's exactly-once argument is the stream sink's, which the failure tests of ADR 0021
  break on purpose; it is not tested again here. `tests/integration/test_cdc_end_to_end.py`
  checks the whole path: replaying the log gives the rows in Postgres, every insert and status
  change once. Restarting Connect and the sink was checked by hand: the watermark arrived, and
  the source's tests (one row per Kafka offset) and staging's passed.
