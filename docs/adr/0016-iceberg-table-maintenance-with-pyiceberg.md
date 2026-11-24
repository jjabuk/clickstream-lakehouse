# ADR 0016: Iceberg table maintenance with PyIceberg, without a Spark engine

- **Status:** accepted
- **Date:** 2026-11-23
- **Versions:** PyIceberg 0.12.0 (the latest release), Lakekeeper 0.15.1, DuckDB 2.0.0.

## Context

Week 3 shows the Iceberg operations a lakehouse relies on: schema evolution, partition
evolution, time travel, compaction and snapshot expiration. In Spark each one is a built-in
procedure (`rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`). This stack has no
Spark: PyIceberg writes and DuckDB reads (ADR 0003). PyIceberg 0.12 covers schema and partition
evolution, time travel by snapshot id, and expiring snapshots from the metadata. It has **no
compaction and no orphan-file removal**. Lakekeeper does not delete data files of expired
snapshots either; it removes files only when a table is purged.

Bronze is small: about 8,000 rows and one ~300 KiB Parquet file per arrival day.

## Decision

The operations live in `ingest/maintenance.py` and run by hand through `make iceberg-*`.
`docs/iceberg-operations.md` shows each one on the dev table with a query before and after.

- **Schema evolution** stays in the load (`ingest.bronze`, `union_by_name`), as in week 1.
- **Partition evolution** (`make iceberg-partition GRAIN=hour|day`) is metadata only. Old files
  keep their spec, new writes use the new one, and DuckDB prunes on both (one file read for an
  hour filter either way). **Bronze stays partitioned by day.** At this volume an hour spec turns
  one 300 KiB file a day into 24 files of 20 KiB, which is the small-file problem compaction
  exists for. The evolution to `hour` is demonstrated and evolved back; going back reuses spec 0.
- **Time travel** (`make iceberg-time-travel DAY=...`) reads the snapshot of main after the
  day's load and before the next arrival day. That snapshot is found from metadata: walk main's
  ancestry and stop at the first snapshot that adds a later arrival day. Maintenance commits are
  skipped. Commit timestamps are wall-clock and say nothing about simulated time, so
  `AT (TIMESTAMP => ...)` is not used.
- **Compaction** (`make iceberg-compact [DAY=...]`) finds arrival days with more than one file in
  the same partition and rewrites each in one overwrite. The rows are read back and written
  unchanged, `_ingested_at` included, all days in one transaction. A rewrite of a named day also
  moves it to the current spec, which is how the hour-partitioned day went back to one file.
  The ingest never fragments a day (one overwrite per day), so `make iceberg-fragment` rewrites a
  day as many appends, the way a streaming sink committing every few minutes would.
- **Snapshot expiration** (`make iceberg-expire KEEP=5`) keeps the newest `KEEP` snapshots of main
  and every branch or tag head. Everything else goes, including snapshots no ref reaches, such
  as those of a failed WAP batch. A count is used rather than an age because commits happen
  minutes apart in a demo. The command reports how many data files no snapshot references any
  more. It does not delete them.

Every maintenance commit carries `maintenance: compaction|fragment` in its snapshot summary, so
the history tells it apart from a load.

## Alternatives considered

- **Spark (or PySpark in a container) for the procedures.** It is the standard toolbox and
  commits compaction as a `replace`. It adds a JVM engine to a stack that has none, for a table
  of a few MB. Stage 2 (Databricks) runs these procedures from Spark (plan, week 7).
- **DuckDB for maintenance.** DuckDB 2.0 writes Iceberg but has no compaction or expiration.
- **Hour partitioning for good.** It would only make sense with far more rows per hour or with a
  query pattern on hours. Bronze is read by day ranges, by dbt's lookback window.
- **Deleting unreferenced files after expiration.** Listing the table's prefix and deleting what
  no snapshot, manifest or metadata file references is easy to get wrong. A file written by a
  commit in progress looks orphaned too, and a mistake loses data. The few MB it would reclaim
  here do not justify the risk. The report shows what is left behind.

## Consequences

- **Compaction is an `overwrite`, not a `replace`.** An incremental reader that follows snapshots
  would see the day change. dbt reads bronze by `server_ts`, so its incremental models are
  unaffected, and `dbt build` passed after the dev demo.
- **Compaction reads a whole day into memory.** That is fine at 8,000 rows a day, not at
  production scale.
- **Maintenance and the load must not run at once.** `publish` refuses to fast-forward if main
  moved after the branch was made (ADR 0008), so a compaction during a load fails that load,
  which can then be re-run. The orchestrator (Airflow) runs them in sequence.
- **Expiration ends time travel to what it removed.** After `KEEP=5` the dev table can no longer
  show 2026-02-06 before its late events; DuckDB answers `Could not find snapshot`.
- **Storage is not reclaimed.** Data files of expired snapshots stay on S3 until the table is
  purged. In week 4 the Kafka sink commits often, compaction will rewrite more, and orphan-file
  cleanup may become worth doing.

> **Update (week 4):** compaction and expiry run on a schedule: the Airflow DAG
> `iceberg_maintenance`, every 30 minutes, `compact` then `expire --keep 20`. Orphan-file removal
> stays out, for the reason above. The stream sink writes to the branch `stream`, which must stay
> a descendant of `main` (ADR 0021). Compaction therefore runs only while `stream` is at `main`,
> skips otherwise, and moves `stream` to the compacted head in the same commit; if the sink
> commits in between, the commit fails and the next run tries again. Maintenance writes only to
> bronze, so it may run while dbt builds.
