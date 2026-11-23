"""Publish the marts from the DuckDB warehouse to Postgres, where the dashboard reads them.

Evidence (ADR 0018) queries a warehouse live and has no connector for a local DuckDB file, and a
DuckDB file has one writer and no server for other processes to read from while dbt writes. So
after each dbt build the marts are copied into the `analytics` database on postgres-platform:

- every `fct_*` and `dim_*` table of the warehouse's `main` schema, as it is;
- into a fresh schema `marts_new`, which then replaces `marts` in one transaction, so a reader
  sees the previous publication or the new one, never half of each;
- readable by `analytics_reader` only (SELECT), the role the dashboard logs in with;
- with `marts._published`: when, and the simulated clock the marts were built against.

The database and the role are created on first use; every step is idempotent.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import duckdb
import psycopg
from psycopg import sql

DATABASE = "analytics"
SCHEMA = "marts"
STAGING_SCHEMA = "marts_new"
READER = "analytics_reader"


@dataclass(frozen=True)
class Publication:
    tables: dict[str, int]  # table -> rows


def _dsn(env: Mapping[str, str], database: str) -> str:
    return (
        f"host={env.get('PLATFORM_POSTGRES_HOST', 'localhost')} "
        f"port={env.get('PLATFORM_POSTGRES_PORT', '5433')} dbname={database} "
        f"user={env['PLATFORM_POSTGRES_USER']} password={env['PLATFORM_POSTGRES_PASSWORD']}"
    )


def ensure_database(env: Mapping[str, str], database: str = DATABASE) -> None:
    """The serving database and its read-only login role."""
    # CREATE DATABASE cannot run in a transaction; connect to the instance's default database.
    with psycopg.connect(_dsn(env, "lakekeeper"), autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", [database])
        if exists.fetchone() is None:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        password = env.get("ANALYTICS_READER_PASSWORD", "analytics-reader-dev")
        role = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", [READER]).fetchone()
        verb = "ALTER" if role else "CREATE"
        conn.execute(
            sql.SQL(verb + " ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(READER), sql.Literal(password)
            )
        )
        conn.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(database), sql.Identifier(READER)
            )
        )


def mart_tables(con: duckdb.DuckDBPyConnection, catalog: str) -> list[str]:
    rows = con.execute(
        """
        select table_name from information_schema.tables
        where table_catalog = ? and table_schema = 'main' and table_type = 'BASE TABLE'
          and (starts_with(table_name, 'fct_') or starts_with(table_name, 'dim_'))
        order by table_name
        """,
        [catalog],
    ).fetchall()
    return [name for (name,) in rows]


def publish(
    env: Mapping[str, str],
    warehouse: Path,
    simulated_now: str | None,
    database: str = DATABASE,
) -> Publication:
    ensure_database(env, database)

    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{warehouse}' AS wh (READ_ONLY)")
    con.execute(f"ATTACH '{_dsn(env, database)}' AS pg (TYPE postgres)")
    tables = mart_tables(con, "wh")
    if not tables:
        raise RuntimeError(f"no fct_ or dim_ tables in {warehouse}; run dbt first")

    con.execute(f"CALL postgres_execute('pg', 'DROP SCHEMA IF EXISTS {STAGING_SCHEMA} CASCADE')")
    con.execute(f"CALL postgres_execute('pg', 'CREATE SCHEMA {STAGING_SCHEMA}')")
    con.execute("CALL postgres_clear_cache()")  # the attach caches the schema list
    counts = {}
    for table in tables:
        con.execute(f"CREATE TABLE pg.{STAGING_SCHEMA}.{table} AS SELECT * FROM wh.main.{table}")
        counts[table] = con.execute(f"SELECT count(*) FROM wh.main.{table}").fetchone()[0]
    con.close()

    with psycopg.connect(_dsn(env, database)) as conn, conn.transaction():
        conn.execute(
            sql.SQL(
                "CREATE TABLE {}._published (published_at timestamptz, simulated_now text)"
            ).format(sql.Identifier(STAGING_SCHEMA))
        )
        conn.execute(
            sql.SQL("INSERT INTO {}._published VALUES (now(), %s)").format(
                sql.Identifier(STAGING_SCHEMA)
            ),
            [simulated_now],
        )
        conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(SCHEMA)))
        conn.execute(
            sql.SQL("ALTER SCHEMA {} RENAME TO {}").format(
                sql.Identifier(STAGING_SCHEMA), sql.Identifier(SCHEMA)
            )
        )
        conn.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(SCHEMA), sql.Identifier(READER)
            )
        )
        conn.execute(
            sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                sql.Identifier(SCHEMA), sql.Identifier(READER)
            )
        )
    return Publication(counts)
