"""CLI: `python -m generator --date 2026-02-10 --seed 42` (normally via `make day`)."""

import argparse
import os
from datetime import date

from generator.config import GeneratorConfig
from generator.day import run_day


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate one day of shop traffic and orders.")
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="arrival day, UTC")
    parser.add_argument("--seed", type=int, default=GeneratorConfig.seed)
    args = parser.parse_args()

    report = run_day(GeneratorConfig(seed=args.seed), args.date, os.environ)
    print(
        f"{report.day}: {report.events} events -> s3://{report.landing_path}; "
        f"{report.orders_created} new orders, {report.status_changes} order status changes"
    )


if __name__ == "__main__":
    main()
