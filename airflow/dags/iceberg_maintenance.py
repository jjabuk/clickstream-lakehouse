"""Iceberg maintenance of bronze on a schedule (ADR 0016): compaction, then snapshot expiry.

The stream sink commits a small file every few seconds, so a streamed day lands as several files.
Every 30 minutes of wall-clock time (the simulated days do not matter here):

1. `compact`: merge the files of fragmented arrival days into one per partition. It runs only
   while the sink's branch `stream` holds nothing unpublished, and moves that branch along, so
   the next publish is still a fast-forward. Otherwise it skips and the next run tries again.
2. `expire`: drop all but the newest snapshots of main (and the heads of branches and tags).
   Time travel to the expired ones ends there. Files are not deleted: PyIceberg deletes none, and
   orphan-file removal is left out on purpose (ADR 0016).

It writes to bronze only, never to the DuckDB warehouse, so it may run while clickstream_daily
builds dbt. A conflict with a load or the sink fails one commit, never corrupts the table.
"""

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT = Path(os.environ.get("PROJECT_DIR", "/opt/project"))
PYTHON = os.environ.get("PROJECT_PYTHON", "/opt/project-venv/bin/python")
MAINTENANCE = f"{PYTHON} -m ingest.maintenance"
# Enough history for a few days of time travel at one stream day a run.
KEEP_SNAPSHOTS = 20

with DAG(
    dag_id="iceberg_maintenance",
    description="Compact bronze's small files and expire old snapshots",
    schedule=timedelta(minutes=30),
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["clickstream", "iceberg"],
) as dag:
    compact = BashOperator(
        task_id="compact", cwd=str(PROJECT), bash_command=f"{MAINTENANCE} compact"
    )
    expire = BashOperator(
        task_id="expire",
        cwd=str(PROJECT),
        bash_command=f"{MAINTENANCE} expire --keep {KEEP_SNAPSHOTS}",
    )

    compact >> expire

    dag.doc_md = __doc__
