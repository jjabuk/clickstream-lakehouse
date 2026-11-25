"""Pipeline alerts in one table (ADR 0026): what broke, how badly, and on which simulated day.

`ops.pipeline_alerts` lives in the serving database next to the marts, so the dashboard's Data
health page reads it with the same read-only role. Three writers:

- the freshness check (`python -m serving.freshness`): a mart behind the simulated clock;
- Airflow's failure callback: a task of `clickstream_daily` that failed (a rejected delivery);
- Airflow's deadline callback: a run that is still not done after its deadline.

The channel is a table on purpose: a webhook to a chat is one more line in the same place.

CLI: `python -m serving.alerts KIND SUBJECT DETAIL [--level error|warn] [--day YYYY-MM-DD]`.
"""

import argparse
import os
from collections.abc import Mapping
from datetime import date

import psycopg
from psycopg import sql

from serving.publish import DATABASE, READER, _dsn, ensure_database

SCHEMA = "ops"
TABLE = "pipeline_alerts"
LEVELS = ("warn", "error")


def ensure_alerts_table(conn: psycopg.Connection) -> None:
    conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA)))
    conn.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                raised_at timestamptz NOT NULL DEFAULT now(),
                simulated_day date,
                level text NOT NULL CHECK (level IN ('warn', 'error')),
                kind text NOT NULL,
                subject text NOT NULL,
                detail text NOT NULL
            )
            """
        ).format(sql.Identifier(SCHEMA), sql.Identifier(TABLE))
    )
    conn.execute(
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
            sql.Identifier(SCHEMA), sql.Identifier(READER)
        )
    )
    conn.execute(
        sql.SQL("GRANT SELECT ON {}.{} TO {}").format(
            sql.Identifier(SCHEMA), sql.Identifier(TABLE), sql.Identifier(READER)
        )
    )


def raise_alert(
    env: Mapping[str, str],
    kind: str,
    subject: str,
    detail: str,
    level: str = "error",
    simulated_day: date | None = None,
    database: str = DATABASE,
) -> None:
    if level not in LEVELS:
        raise ValueError(f"level must be one of {LEVELS}, not {level!r}")
    insert = sql.SQL(
        "INSERT INTO {}.{} (simulated_day, level, kind, subject, detail) "
        "VALUES (%s, %s, %s, %s, %s)"
    ).format(sql.Identifier(SCHEMA), sql.Identifier(TABLE))
    values = [simulated_day, level, kind, subject, detail]
    # The setup only when the table is missing: callbacks of tasks that failed together run at
    # the same moment, and concurrent GRANTs on the same object fail in Postgres.
    try:
        with psycopg.connect(_dsn(env, database)) as conn, conn.transaction():
            conn.execute(insert, values)
            return
    except (psycopg.errors.UndefinedTable, psycopg.errors.InvalidSchemaName):
        pass  # no table yet
    except psycopg.OperationalError:
        pass  # no database yet
    ensure_database(env, database)
    with psycopg.connect(_dsn(env, database)) as conn, conn.transaction():
        ensure_alerts_table(conn)
        conn.execute(insert, values)


def main() -> None:
    parser = argparse.ArgumentParser(description="Record a pipeline alert.")
    parser.add_argument("kind", help="freshness, task_failed, deadline_missed, ...")
    parser.add_argument("subject", help="what the alert is about: a mart, a task, a DAG")
    parser.add_argument("detail")
    parser.add_argument("--level", choices=LEVELS, default="error")
    parser.add_argument("--day", type=date.fromisoformat, help="the simulated day")
    args = parser.parse_args()
    raise_alert(os.environ, args.kind, args.subject, args.detail, args.level, args.day)
    print(f"alert: [{args.level}] {args.kind} {args.subject}: {args.detail}")


if __name__ == "__main__":
    main()
