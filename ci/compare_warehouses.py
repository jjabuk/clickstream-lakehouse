"""Fail if two DuckDB warehouses differ in any table of the `main` schema.

Used by `make ci-verify-incremental`: the warehouse built day by day (incremental runs, late
events arriving between them) must equal the same warehouse after a full refresh of the
incremental models. Rows are compared as multisets (EXCEPT ALL both ways), so duplicates count.

    python ci/compare_warehouses.py data/ci/incremental.duckdb data/ci/warehouse.duckdb
"""

import sys

import duckdb

SCHEMA = "main"


def tables(con: duckdb.DuckDBPyConnection, db: str) -> set[str]:
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = ? AND table_type = 'BASE TABLE'",
        [db, SCHEMA],
    ).fetchall()
    return {r[0] for r in rows}


def main(left_path: str, right_path: str) -> int:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"ATTACH '{left_path}' AS l (READ_ONLY)")
    con.execute(f"ATTACH '{right_path}' AS r (READ_ONLY)")

    left, right = tables(con, "l"), tables(con, "r")
    failures = [f"table only in {left_path}: {t}" for t in sorted(left - right)]
    failures += [f"table only in {right_path}: {t}" for t in sorted(right - left)]

    for table in sorted(left & right):
        # Column order may differ (on_schema_change appends new columns at the end).
        cols = [
            r[0]
            for r in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_catalog = 'l' AND table_schema = ? AND table_name = ? "
                "ORDER BY ordinal_position",
                [SCHEMA, table],
            ).fetchall()
        ]
        select = ", ".join(f'"{c}"' for c in cols)
        l_rel, r_rel = f'l.{SCHEMA}."{table}"', f'r.{SCHEMA}."{table}"'
        only_l, only_r, n = con.execute(
            f"SELECT "
            f"(SELECT count(*) FROM (SELECT {select} FROM {l_rel} EXCEPT ALL "
            f"SELECT {select} FROM {r_rel})), "
            f"(SELECT count(*) FROM (SELECT {select} FROM {r_rel} EXCEPT ALL "
            f"SELECT {select} FROM {l_rel})), "
            f"(SELECT count(*) FROM {l_rel})"
        ).fetchone()
        status = "OK" if only_l == only_r == 0 else "DIFF"
        print(f"{status:4} {table}: {n} rows, {only_l} only left, {only_r} only right")
        if status == "DIFF":
            failures.append(f"{table} differs")

    for f in failures:
        print(f"FAIL {f}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1], sys.argv[2]))
