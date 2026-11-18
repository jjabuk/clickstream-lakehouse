"""CLI: `python -m ingest --date 2026-02-10` loads that day's landing file into bronze."""

import argparse
import os
from datetime import date

from ingest import landing
from ingest.bronze import lakehouse_catalog, load_day


def main() -> None:
    parser = argparse.ArgumentParser(description="Load one landing day into the bronze table.")
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="arrival day, UTC")
    args = parser.parse_args()

    env = os.environ
    result = load_day(
        lakehouse_catalog(env),
        landing.filesystem(env),
        landing.landing_path(env, args.date),
        args.date,
    )
    note = f"; new columns: {', '.join(result.new_columns)}" if result.new_columns else ""
    print(f"{args.date}: {result.rows} rows in bronze.clickstream_events{note}")


if __name__ == "__main__":
    main()
