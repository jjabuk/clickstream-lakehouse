"""CLI: `python -m serving` publishes the marts of the warehouse to Postgres (ADR 0018)."""

import argparse
import os
from pathlib import Path

from serving.publish import DATABASE, SCHEMA, publish


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish the marts to the serving Postgres.")
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=Path(os.environ.get("DBT_DUCKDB_PATH", "data/warehouse.duckdb")),
    )
    args = parser.parse_args()

    env = os.environ
    result = publish(env, args.warehouse, env.get("SIMULATED_NOW") or None)
    rows = sum(result.tables.values())
    print(f"published {len(result.tables)} tables ({rows} rows) to {DATABASE}.{SCHEMA}")
    for table, n in result.tables.items():
        print(f"  {table}: {n}")


if __name__ == "__main__":
    main()
