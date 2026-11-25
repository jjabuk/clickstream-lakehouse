"""One arrival day of the shop: generate it, load it through write-audit-publish, build dbt.

A run handles one arrival day D, the same work as `make day DATE=D` followed by `make dbt-build`
(ADR 0017), then `make marts-publish`:

1. `load_day`: the generator writes D's orders and its clickstream, which reaches bronze through
   write-audit-publish in one of two ways (param `ingest`):
   - `files` (default): a landing file, loaded on a branch, audited and published (ADR 0008);
   - `stream`: Avro events to Kafka; the sink appends them to the branch `stream`, and the task
     waits for the closed window, audits it and fast-forwards main (ADR 0021). It needs
     `make stream-up`.
   A failed audit fails this task, so dbt never runs on a day `main` does not have.
   D's orders reach bronze through CDC (Debezium, ADR 0007); the task ends by waiting for a
   watermark, so the change log holds every order change the generator made.
   `make airflow-day DAY=... FAULT=truncated` breaks the first delivery on purpose; clearing the
   failed task (a retry) is the source's corrected redelivery.
2. `dbt`: one `dbt build` (Cosmos watcher mode, dbt v2 as a subprocess, ADR 0024), with a task
   per model that follows its status. DuckDB allows one writer per file, so the build cannot be
   split into parallel dbt processes.
3. `publish_marts`: `make marts-publish`, the marts copied to Postgres for Evidence (ADR 0018).
   It runs only after a passing build, so the dashboard never shows a failed day.
4. `freshness`: the marts' SLA against the simulated clock (`make freshness`, ADR 0026). It runs
   whatever happened before it, so a day that never reached the marts shows up as stale marts.

Alerts (ADR 0026) go to `ops.pipeline_alerts`, which the dashboard's Data health page shows: a
stale mart (from `freshness`), a failed task (`on_failure_callback`), and a run still not done
`DEADLINE_MINUTES` after it was queued (a deadline alert).

Simulated time: the shop's days are not today's days, so only two kinds of run do anything.
Every run handles its logical date: a backfill run (`make airflow-backfill FROM=... TO=...`), or a
manual run triggered with that logical date and the same `day` (`make airflow-day DAY=...`), so
`depends_on_past` orders both among the simulated days. `replays_only` skips the rest: the
scheduled run an unpaused DAG gets for the latest wall-clock day, and a manual run that does not
name its day, whose logical date is the wall clock.
Runs go strictly one after another: bronze's volume check and the incremental models need the
previous day in place.
"""

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG, DeadlineAlert, DeadlineReference, Param, SyncCallback
from airflow.timetables.interval import CronDataIntervalTimetable
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    InvocationMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from pipeline_alerts import POSTGRES, task_failed

PROJECT = Path(os.environ.get("PROJECT_DIR", "/opt/project"))
PYTHON = os.environ.get("PROJECT_PYTHON", "/opt/project-venv/bin/python")
DBT = os.environ.get("DBT_EXECUTABLE", "/opt/project-venv/bin/dbt")
DBT_PROJECT = PROJECT / "dbt"

DAY = "{{ logical_date | ds }}"
# The simulated clock after a day: its end, as `make day` sets it (data/simulated_now). The
# published marts record it.
SIMULATED_NOW = "{{ macros.ds_add(logical_date | ds, 1) }}T00:00:00+00:00"
# A day takes about two minutes; a run still going after this many has a problem worth an alert.
DEADLINE_MINUTES = 4


def _is_replay(dag_run, params, **_) -> bool:
    """A backfill's logical date is a simulated day; a manual run must name it in `day` too.

    A manual run triggered without a logical date may have none in its context.
    """
    if dag_run.run_type == "backfill":
        return True
    logical = dag_run.logical_date
    return (
        dag_run.run_type == "manual"
        and logical is not None
        and params.get("day") == logical.date().isoformat()
    )


profile = ProfileConfig(
    profile_name="clickstream",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT / "profiles.yml",
)

with DAG(
    dag_id="clickstream_daily",
    description="Generate, audit and publish one arrival day, then build the dbt project",
    # Classic data intervals, so a backfill's logical date is the day it handles.
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 1, 10, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"depends_on_past": True, "retries": 0, "on_failure_callback": task_failed},
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=DEADLINE_MINUTES),
        callback=SyncCallback(
            "pipeline_alerts.deadline_missed", kwargs={"minutes": DEADLINE_MINUTES}
        ),
    ),
    params={
        "day": Param(
            None,
            type=["null", "string"],
            format="date",
            description="a manual run's arrival day, equal to its logical date",
        ),
        "seed": Param(42, type="integer", description="generator seed"),
        "ingest": Param(
            "files",
            enum=["files", "stream"],
            description="landing files, or Kafka and the stream sink (needs make stream-up)",
        ),
        "fault": Param(
            None,
            type=["null", "string"],
            enum=[None, "truncated", "duplicated", "bad_event_type"],
            description="break the first delivery on purpose; the audit must stop it",
        ),
    },
    tags=["clickstream"],
) as dag:
    replays_only = ShortCircuitOperator(
        task_id="replays_only",
        python_callable=_is_replay,
        # A scheduled run must skip at once. Waiting for the previous logical date, it would wait
        # for a replay of an earlier simulated day that cannot start while this run holds the
        # only active slot. load_day keeps the order among the replays.
        depends_on_past=False,
    )

    load_day = BashOperator(
        task_id="load_day",
        cwd=str(PROJECT),
        bash_command=(
            f"set -e; {PYTHON} -m generator --date {DAY} --seed {{{{ params.seed }}}}"
            "{% if params.ingest == 'stream' %} --stream{% endif %}"
            # Only the first try: a retry is the corrected redelivery.
            "{% if params.fault and ti.try_number == 1 %} --fault {{ params.fault }}{% endif %}; "
            "{% if params.ingest == 'stream' %}"
            f"{PYTHON} -m ingest.stream publish --date {DAY}; "
            "{% else %}"
            f"{PYTHON} -m ingest --date {DAY}; "
            "{% endif %}"
            # The day's orders arrive through CDC: wait until the shop's change log has them.
            f"{PYTHON} -m ingest.cdc wait; "
            f"{PYTHON} ci/advance_clock.py {DAY} data/simulated_now"
        ),
    )

    dbt = DbtTaskGroup(
        group_id="dbt",
        # dbt_packages comes with the mounted repository (`make airflow-up` installs it). Cosmos
        # runs dbt in a temporary copy of the project, which by default links dbt_packages but
        # leaves out package-lock.yml, and dbt then refuses the transitive dbt_date package.
        # Copying both keeps them together.
        project_config=ProjectConfig(DBT_PROJECT, install_dbt_deps=False, copy_dbt_packages=True),
        profile_config=profile,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER,
            # dbt lives in the project's venv, not in Airflow's environment.
            invocation_mode=InvocationMode.SUBPROCESS,
            dbt_executable_path=DBT,
        ),
        render_config=RenderConfig(
            dbt_executable_path=DBT,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        operator_args={
            # load_day already waits for the previous day, and the run waits for load_day. With
            # depends_on_past here, the tasks of a model added later never start: the previous
            # run has no instance of them.
            "depends_on_past": False,
        },
    )

    # The marts for Evidence (ADR 0018): a copy in Postgres, swapped in one transaction.
    publish_marts = BashOperator(
        task_id="publish_marts",
        cwd=str(PROJECT),
        bash_command=f"{PYTHON} -m serving",
        env={"SIMULATED_NOW": SIMULATED_NOW, **POSTGRES},
        append_env=True,
        # As for the dbt tasks: load_day orders the days.
        depends_on_past=False,
    )

    # The SLA check (ADR 0026), after every replayed day whatever happened before it. A scheduled
    # run that replays_only skips skips this too: its wall-clock day has no data to be fresh for.
    freshness = BashOperator(
        task_id="freshness",
        cwd=str(PROJECT),
        bash_command=(
            f"{PYTHON} -m serving.freshness --as-of {SIMULATED_NOW} --day {DAY} --dbt {DBT}"
        ),
        env=POSTGRES,
        append_env=True,
        trigger_rule="all_done",
        depends_on_past=False,
    )

    replays_only >> load_day >> dbt >> publish_marts >> freshness

    dag.doc_md = __doc__
