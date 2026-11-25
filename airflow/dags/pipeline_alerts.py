"""Airflow callbacks that record pipeline alerts in `ops.pipeline_alerts` (ADR 0026).

Not a DAG: the callbacks of `clickstream_daily`. They run in Airflow's own environment, which
does not have the project's dependencies, so each one hands the alert to `python -m
serving.alerts` in the project's virtualenv. A callback never raises: a lost alert must not hide
the failure that caused it, which the task's state and log still show.
"""

import logging
import os
import subprocess

PYTHON = os.environ.get("PROJECT_PYTHON", "/opt/project-venv/bin/python")
PROJECT = os.environ.get("PROJECT_DIR", "/opt/project")
# The task containers reach postgres-platform by its compose name, not the host's published port.
POSTGRES = {"PLATFORM_POSTGRES_HOST": "postgres-platform", "PLATFORM_POSTGRES_PORT": "5432"}

# Cosmos's watcher mode: the task that runs `dbt build`; the model tasks report its results.
WATCHER = "dbt.dbt_producer_watcher"

log = logging.getLogger(__name__)


def _record(kind: str, subject: str, detail: str, day: str | None) -> None:
    command = [PYTHON, "-m", "serving.alerts", kind, subject, detail]
    if day:
        command += ["--day", day]
    try:
        subprocess.run(command, cwd=PROJECT, env={**os.environ, **POSTGRES}, check=True, timeout=60)
    except Exception:
        log.exception("could not record the alert %s %s", kind, subject)


def _simulated_day(dag_run) -> str | None:
    logical = getattr(dag_run, "logical_date", None)
    if not logical:
        return None
    return str(logical)[:10] if isinstance(logical, str) else logical.date().isoformat()


def task_failed(context) -> None:
    """on_failure_callback: a failed task of a replayed day (a rejected delivery, a dbt error)."""
    ti = context["ti"]
    if ti.task_id == "freshness":
        return  # the freshness check records its own alerts, one per stale mart
    if ti.task_id.startswith("dbt.") and ti.task_id != WATCHER:
        # Cosmos's model tasks follow the one dbt build: when it fails, they all fail with it.
        # One alert, the build's, says it; one per model would bury it.
        return
    error = context.get("exception")
    detail = f"{ti.task_id} failed (try {ti.try_number})" + (f": {error}" if error else "")
    _record(
        "task_failed",
        f"{ti.dag_id}.{ti.task_id}",
        detail[:500],
        _simulated_day(context.get("dag_run")),
    )


def deadline_missed(**kwargs) -> None:
    """Deadline callback: the run was still not done when its deadline passed.

    Airflow passes the run serialized, as a dict, under `context["dag_run"]`.
    """
    run = (kwargs.get("context") or {}).get("dag_run") or {}
    day = str(run.get("logical_date") or "")[:10] or None
    detail = (
        f"run {run.get('dag_run_id', '?')} not finished {kwargs.get('minutes')} min after it "
        f"was queued at {run.get('queued_at', '?')}"
    )
    _record("deadline_missed", run.get("dag_id", "clickstream_daily"), detail, day)
