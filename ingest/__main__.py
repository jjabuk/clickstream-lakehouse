"""CLI: `python -m ingest --date 2026-02-10` loads that day's landing file into bronze.

The load is write-audit-publish (ADR 0008): exit code 1 means the audit rejected the batch, which
is kept on its branch while `main` is unchanged.
"""

import argparse
import os
import sys
from datetime import date

from ingest import landing
from ingest.bronze import AuditFailed, lakehouse_catalog, load_day


def main() -> None:
    parser = argparse.ArgumentParser(description="Load one landing day into the bronze table.")
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="arrival day, UTC")
    args = parser.parse_args()

    env = os.environ
    try:
        result = load_day(
            lakehouse_catalog(env),
            landing.filesystem(env),
            landing.landing_path(env, args.date),
            args.date,
        )
    except AuditFailed as failure:
        print(
            f"{failure.day}: audit failed, main unchanged; batch kept on branch "
            f"'{failure.branch}' ({failure.result.rows} rows):",
            file=sys.stderr,
        )
        for violation in failure.result.violations:
            print(f"  - {violation}", file=sys.stderr)
        sys.exit(1)
    note = f"; new columns: {', '.join(result.new_columns)}" if result.new_columns else ""
    print(
        f"{args.date}: {result.rows} rows audited and published to bronze.clickstream_events "
        f"(snapshot {result.snapshot_id}){note}"
    )


if __name__ == "__main__":
    main()
