"""CLI: `python -m generator --date 2026-02-10 --seed 42` (normally via `make day`)."""

import argparse
import os
from datetime import date

from generator.config import GeneratorConfig
from generator.day import run_day
from generator.faults import Fault


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate one day of shop traffic and orders.")
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="arrival day, UTC")
    parser.add_argument("--seed", type=int, default=GeneratorConfig.seed)
    parser.add_argument(
        "--fault",
        type=Fault,
        choices=list(Fault),
        help="deliver a broken batch, to exercise write-audit-publish (ADR 0008)",
    )
    parser.add_argument(
        "--stream",
        action="store_true",
        help="send the events to Kafka instead of writing a landing file (ADR 0005)",
    )
    args = parser.parse_args()

    report = run_day(
        GeneratorConfig(seed=args.seed), args.date, os.environ, fault=args.fault, stream=args.stream
    )
    fault = f" (fault: {args.fault})" if args.fault else ""
    dest = report.landing_path if args.stream else f"s3://{report.landing_path}"
    print(
        f"{report.day}: {report.events} events -> {dest}{fault}; "
        f"{report.orders_created} new orders, {report.status_changes} order status changes"
    )


if __name__ == "__main__":
    main()
