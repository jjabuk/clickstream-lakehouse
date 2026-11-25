"""The marts' freshness SLA, checked against the simulated clock and recorded as alerts (ADR 0026).

The SLA is defined once, in the model YAML: the generic test `fresh_as_of_simulated_now` with its
warn and error thresholds. This runs those tests (tag `freshness`) for a given `as_of`, and turns
every warning or failure into a row of `ops.pipeline_alerts`, one per mart. It exits non-zero
when a mart is past its error threshold.

`dbt build` runs the same tests against SIMULATED_NOW, but only when the build runs. The DAG runs
this check after every replayed day whatever happened before it, so a day that never reached the
marts (a rejected delivery nobody redelivered) shows up as stale marts.

CLI: `python -m serving.freshness --as-of 2026-02-21T00:00:00+00:00 [--day 2026-02-20]`.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

from serving.alerts import raise_alert

PROJECT = Path(__file__).resolve().parents[1] / "dbt"


def _model_of(test: dict, nodes: dict) -> str:
    attached = test.get("attached_node") or next(iter(test["depends_on"]["nodes"]), "")
    node = nodes.get(attached, {})
    version = node.get("version")
    return node.get("name", attached) + (f" v{version}" if version is not None else "")


def check(as_of: str, dbt: str) -> list[tuple[str, str, str]]:
    """Run the freshness tests; returns (level, mart, message) for each warning or failure."""
    with tempfile.TemporaryDirectory() as target:
        subprocess.run(
            [
                dbt,
                "test",
                "--project-dir",
                str(PROJECT),
                "--profiles-dir",
                str(PROJECT),
                "--target-path",
                target,  # not the project's target/: the manifest there is CI's state
                "--select",
                "tag:freshness",
                "--vars",
                json.dumps({"as_of": as_of}),
            ],
            check=False,  # a failing test is a result, read from run_results.json
        )
        results = json.loads((Path(target) / "run_results.json").read_text())["results"]
        nodes = json.loads((Path(target) / "manifest.json").read_text())["nodes"]
    found = []
    for result in results:
        if result["status"] not in ("warn", "fail", "error"):
            continue
        level = "warn" if result["status"] == "warn" else "error"
        test = nodes[result["unique_id"]]
        kwargs = test["test_metadata"]["kwargs"]
        hours = kwargs["warn_after_hours" if level == "warn" else "error_after_hours"]
        message = f"latest {kwargs['column_name']} is more than {hours} h behind {as_of}"
        found.append((level, _model_of(test, nodes), message))
    return found


def main() -> None:
    parser = argparse.ArgumentParser(description="Check the marts' freshness SLA, record alerts.")
    parser.add_argument("--as-of", required=True, help="the simulated now, an ISO timestamp")
    parser.add_argument("--day", type=date.fromisoformat, help="the simulated day for the alerts")
    parser.add_argument(
        "--dbt", default=str(Path(sys.executable).parent / "dbt"), help="the dbt executable"
    )
    args = parser.parse_args()

    findings = check(args.as_of, args.dbt)
    for level, mart, message in findings:
        raise_alert(os.environ, "freshness", mart, message, level, args.day)
        print(f"freshness: [{level}] {mart}: {message}")
    if any(level == "error" for level, _, _ in findings):
        sys.exit(1)
    if not findings:
        print(f"freshness: every mart is fresh as of {args.as_of}")


if __name__ == "__main__":
    main()
