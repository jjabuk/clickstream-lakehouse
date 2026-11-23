"""One arrival day of the shop: generate it, load it through write-audit-publish, build dbt.

A run handles one arrival day D, the same work as `make day DATE=D` followed by `make dbt-build`
(ADR 0017), then `make marts-publish`:

1. `load_day`: the generator writes D's landing file and orders, the ingest loads it to bronze on
   a branch, audits it and publishes it (ADR 0008). A failed audit fails this task, so dbt never
   runs on a day `main` does not have. `make airflow-day DAY=... FAULT=truncated` breaks the first
   delivery on purpose; clearing the failed task (a retry) is the source's corrected redelivery.
2. `elementary_tables`: elementary's own tables, which its run hooks write to.
3. `dbt`: one `dbt build` (Cosmos watcher mode), with a task per model that follows its status.
   DuckDB allows one writer per file, so the build cannot be split into parallel dbt processes.
4. `publish_marts`: `make marts-publish`, the marts copied to Postgres for Evidence (ADR 0018).
   It runs only after a passing build, so the dashboard never shows a failed day.

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
from datetime import UTC, datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG, Param
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

PROJECT = Path(os.environ.get("PROJECT_DIR", "/opt/project"))
PYTHON = os.environ.get("PROJECT_PYTHON", "/opt/project-venv/bin/python")
DBT = os.environ.get("DBT_EXECUTABLE", "/opt/project-venv/bin/dbt")
DBT_PROJECT = PROJECT / "dbt"

DAY = "{{ logical_date | ds }}"
# elementary's "now" is the end of the arrival day, as `make day` sets it (data/simulated_now).
SIMULATED_NOW = "{{ macros.ds_add(logical_date | ds, 1) }}T00:00:00+00:00"


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
    default_args={"depends_on_past": True, "retries": 0},
    params={
        "day": Param(
            None,
            type=["null", "string"],
            format="date",
            description="a manual run's arrival day, equal to its logical date",
        ),
        "seed": Param(42, type="integer", description="generator seed"),
        "fault": Param(
            None,
            type=["null", "string"],
            enum=[None, "truncated", "duplicated", "bad_event_type"],
            description="break the first delivery on purpose; the audit must stop it",
        ),
    },
    tags=["clickstream"],
) as dag:
    replays_only = ShortCircuitOperator(task_id="replays_only", python_callable=_is_replay)

    load_day = BashOperator(
        task_id="load_day",
        cwd=str(PROJECT),
        bash_command=(
            f"set -e; {PYTHON} -m generator --date {DAY} --seed {{{{ params.seed }}}}"
            # Only the first try: a retry is the corrected redelivery.
            "{% if params.fault and ti.try_number == 1 %} --fault {{ params.fault }}{% endif %}; "
            f"{PYTHON} -m ingest --date {DAY}; "
            f"{PYTHON} ci/advance_clock.py {DAY} data/simulated_now"
        ),
    )

    elementary_tables = BashOperator(
        task_id="elementary_tables",
        cwd=str(PROJECT),
        bash_command=(
            f"{DBT} run --project-dir dbt --profiles-dir dbt --select package:elementary "
            "--quiet --log-path /tmp/dbt-logs --target-path /tmp/dbt-target"
        ),
        env={"SIMULATED_NOW": SIMULATED_NOW},
        append_env=True,
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
            # The project's own models, snapshot and tests; elementary's models are built above.
            exclude=["package:elementary"],
        ),
        operator_args={
            "env": {"SIMULATED_NOW": SIMULATED_NOW},
            "append_env": True,
        },
    )

    # The marts for Evidence (ADR 0018): a copy in Postgres, swapped in one transaction.
    publish_marts = BashOperator(
        task_id="publish_marts",
        cwd=str(PROJECT),
        bash_command=f"{PYTHON} -m serving",
        env={
            "SIMULATED_NOW": SIMULATED_NOW,
            "PLATFORM_POSTGRES_HOST": "postgres-platform",
            "PLATFORM_POSTGRES_PORT": "5432",
        },
        append_env=True,
        # The dbt tasks before it already wait for the previous day. With depends_on_past, a task
        # added to the DAG later never starts: the previous run has no instance of it.
        depends_on_past=False,
    )

    replays_only >> load_day >> elementary_tables >> dbt >> publish_marts

    dag.doc_md = __doc__
