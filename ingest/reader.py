"""DuckDB attached to the lakehouse the way dbt reads it (dbt/profiles.yml, ADR 0003).

Used by the Iceberg maintenance CLI to show the reader's view of an operation, and by the
integration tests.
"""

from collections.abc import Mapping

import duckdb


def lakehouse_duckdb(
    env: Mapping[str, str], *, read_only: bool = True
) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB with the catalog attached as `lk`, signing with its own S3 key."""
    con = duckdb.connect()
    con.execute("INSTALL iceberg; INSTALL httpfs; LOAD iceberg; LOAD httpfs;")
    con.execute("SET TimeZone = 'UTC'")
    endpoint = env["S3_ENDPOINT"].removeprefix("http://")
    con.execute(
        f"""CREATE SECRET s3 (
            TYPE s3, KEY_ID '{env["S3_ACCESS_KEY_ID"]}', SECRET '{env["S3_SECRET_ACCESS_KEY"]}',
            ENDPOINT '{endpoint}', URL_STYLE 'path', USE_SSL false,
            REGION '{env.get("S3_REGION", "garage")}')"""
    )
    base = env.get("LAKEKEEPER_URL", "http://localhost:8181")
    mode = "READ_ONLY, " if read_only else ""
    con.execute(
        f"""ATTACH 'lakehouse' AS lk (
            TYPE iceberg, URI '{base}/catalog', {mode}
            AUTHORIZATION_TYPE none, ACCESS_DELEGATION_MODE 'none')"""
    )
    return con
