# Iceberg operations on bronze

Each operation was run on the dev table `bronze.clickstream_events`, one arrival day per load. The
queries are DuckDB, the engine dbt reads with, with the catalog attached as `lk` as in
`dbt/profiles.yml`. The writes go through PyIceberg
(`ingest/maintenance.py`). Why compaction and expiration look the way they do: [ADR
0016](adr/0016-iceberg-table-maintenance-with-pyiceberg.md).

Run them in this order: time travel needs the snapshots that expiration removes.

## 1. Schema evolution: `utm_campaign` appears on 2026-02-01

The generator starts sending `utm_campaign` on 2026-02-01 (`utm_start` in the generator config).
The load adds the column with `union_by_name` (`ingest/bronze.py`). It writes no data files for
the older days, which read the column as NULL.

```sql
select cast(server_ts as date) as arrival_day, count(utm_campaign) as with_campaign
from lk.bronze.clickstream_events
where cast(server_ts as date) between '2026-01-31' and '2026-02-02'
group by all order by all;
```

| arrival_day | before 2026-02-01 was loaded | after |
|---|---|---|
| 2026-01-31 | *(no such column)* | 0 |
| 2026-02-01 |  | 3264 |
| 2026-02-02 |  | 2451 |

elementary's `schema_changes` test on the source warns about it on that day.

## 2. Partition evolution: `day(server_ts)` → `hour(server_ts)`

```bash
make iceberg-partition GRAIN=hour
```
```bash
make day DATE=2026-02-07
```
```bash
make iceberg-files
```

Metadata only: no file is rewritten. Files of the old spec (0) stay. The next day lands in the
new spec (1):

| arrival_day | spec_id | files | records | avg KiB |
|---|---|---|---|---|
| 2026-02-06 | 0 | 1 | 8053 | 308 |
| 2026-02-07 | 1 | **24** | 8232 | **20** |

Readers prune on both specs. One hour of each day, with the files DuckDB read (from `explain
analyze`):

```sql
select count(*) from lk.bronze.clickstream_events
where server_ts >= '2026-02-07 10:00' and server_ts < '2026-02-07 11:00';
```

| day | spec | files read | file |
|---|---|---|---|
| 2026-02-06 | day | 1 | `server_ts_day=2026-02-06/…` (the whole day) |
| 2026-02-07 | hour | 1 | `server_ts_hour=2026-02-07-10/…` |

At ~8,000 rows a day the hour spec only produces small files, so bronze goes back to `day`
(`make iceberg-partition GRAIN=day`). That evolution reuses spec 0. 2026-02-07 keeps its 24
files until compaction (section 4).

## 3. Time travel: a day before its late events arrived

An event can arrive days after it happened, so 2026-02-06 keeps growing after its own load.
`make iceberg-time-travel DAY=2026-02-06` finds the snapshot of main after that day's load and
before the next one (`snapshot_as_of`), and runs:

```sql
select cast(server_ts as date) as arrival_day, count(*) as events
from lk.bronze.clickstream_events at (version => 6993704786740758323)  -- drop AT for "now"
where cast(event_ts as date) = '2026-02-06'
group by all order by arrival_day;
```

| arrival_day | as of snapshot 6993704786740758323 | now |
|---|---|---|
| 2026-02-06 | 7870 | 7870 |
| 2026-02-07 | | **124** (late) |

The 124 rows are what dbt's lookback window (ADR 0004) folds into 2026-02-06's sessions.

## 4. Compaction

The daily load writes one file per day, so the small files of a streaming sink are simulated
here. (Since week 4 the stream sink leaves them for real, and the Airflow DAG
`iceberg_maintenance` compacts them on a schedule, ADR 0016.) `make iceberg-fragment DAY=2026-02-05 PARTS=24` rewrites the day as 24 appends:

| arrival_day | spec_id | files | records | avg KiB |
|---|---|---|---|---|
| 2026-02-05 | 0 | **24** | 7956 | 20 |
| 2026-02-07 | 1 | 24 | 8232 | 20 |

`make iceberg-compact` merges the days that have more than one file in a partition.
`make iceberg-compact DAY=2026-02-07` also rewrites the hour-partitioned day into the current
spec:

```
2026-02-05: 24 -> 1 data files
2026-02-07: 24 -> 1 data files
```

The rows did not change. Here the table after compaction is compared with the snapshot before
it, in both directions:

```sql
select count(*) from (
  (select * from lk.bronze.clickstream_events
   except all select * from lk.bronze.clickstream_events at (version => 2564140707840797718))
  union all
  (select * from lk.bronze.clickstream_events at (version => 2564140707840797718)
   except all select * from lk.bronze.clickstream_events));
-- 0
```

PyIceberg has no `rewrite_data_files`, so this is an `overwrite` (delete and append in one
transaction, tagged `maintenance: compaction` in the snapshot summary) rather than Spark's
`replace`.

## 5. Snapshot expiration

`make iceberg-snapshots` before:

```
41 snapshots on main, 42 in the metadata (one from a failed WAP batch)
```

`make iceberg-expire KEEP=5`:

```
expired 37 snapshots, kept the newest 5 of main and the ref heads; 3 data files (0.7 MiB) are
no longer referenced and stay on S3 (PyIceberg deletes no files)
```

```sql
select count(*) from iceberg_snapshots('lk.bronze.clickstream_events');  -- 42 before, 5 after
```

Time travel from section 3 now fails. This is the price of expiration:

```sql
select count(*) from lk.bronze.clickstream_events at (version => 6993704786740758323);
-- Error: Could not find snapshot with id 6993704786740758323
```

```
$ make iceberg-time-travel DAY=2026-02-06
2026-02-06: its load is no longer in the history of main (expired?), so there is no state before
later days to travel to
```

The current state is unchanged, and `make dbt-build` passed after all five operations.
