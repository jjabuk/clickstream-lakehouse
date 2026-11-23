"""Evidence's metrics give the same numbers as the dbt semantic layer (ADR 0023).

The dashboard defines its metrics in `evidence/metrics/*.yaml`, MetricFlow in the dbt project.
Evidence has no MetricFlow connector, so the definitions exist twice; this test is what keeps
them equal. Both sides run on the same warehouse file: Evidence's SQL over the marts (in a
schema named `marts`, as in Postgres), MetricFlow through `mf query`. The Postgres copy itself is
covered by test_serving.py.
"""

import csv
import os
import re
import subprocess
from pathlib import Path

import duckdb
import pytest
import yaml

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE = Path(os.environ.get("DBT_DUCKDB_PATH", ROOT / "data" / "warehouse.duckdb")).resolve()

# Evidence metric -> MetricFlow metric, per metrics view.
PAIRS = {
    "funnel": {
        "sessions": "sessions",
        "bounce_rate": "bounce_rate",
        "product_rate": "product_rate",
        "cart_rate": "cart_rate",
        "checkout_rate": "checkout_rate",
    },
    "traffic": {"events": "events", "bot_events": "bot_events", "bot_share": "bot_share"},
    "conversion": {
        "product_viewer_days": "product_viewer_days",
        "converted_viewer_days": "converted_viewer_days",
        "visitor_conversion_7d": "visitor_product_to_checkout_7d",
    },
}


def _expand(name: str, metrics: dict) -> str:
    """An Evidence metric as SQL: `{ref}` replaced by the referenced metric, filters folded in."""
    metric = metrics[name]
    sql = metric["sql"]
    if "filter" in metric:
        sql = f"{sql} filter (where {metric['filter']})"
    return re.sub(r"\{(\w+)\}", lambda m: f"({_expand(m.group(1), metrics)})", sql)


def _evidence(view: str, names: list[str]) -> dict[str, dict[str, float]]:
    spec = yaml.safe_load((ROOT / "evidence" / "metrics" / f"{view}.yaml").read_text())
    selects = ", ".join(f"{_expand(n, spec['metrics'])} as {n}" for n in names)
    con = duckdb.connect()
    con.execute(f"attach '{WAREHOUSE}' as wh (read_only)")
    con.execute("create schema marts")
    for (table,) in con.execute(
        "select table_name from information_schema.tables where table_catalog = 'wh' "
        "and table_schema = 'main' and table_type = 'BASE TABLE'"
    ).fetchall():
        con.execute(f"create view marts.{table} as select * from wh.main.{table}")
    rows = con.execute(
        f"select cast({spec['date']} as varchar) as day, {selects} "
        f"from ({spec['base_sql']}) group by 1"
    ).fetchall()
    return {day: dict(zip(names, values, strict=True)) for day, *values in rows}


# MetricFlow has its own environment: it is built on dbt-core 1.x, which cannot live next to v2.
MF = ["uv", "run", "--frozen", "--project", str(ROOT / "tools/metricflow"), "mf"]


def _metricflow(names: list[str], tmp_path: Path) -> dict[str, dict[str, float]]:
    out = tmp_path / "mf.csv"
    env = {**os.environ, "DBT_PROFILES_DIR": ".", "DBT_DUCKDB_PATH": str(WAREHOUSE)}
    subprocess.run(
        ["uv", "run", "dbt", "parse", "--quiet", "--project-dir", ".", "--profiles-dir", "."],
        cwd=ROOT / "dbt",
        env=env,
        check=True,
    )
    subprocess.run(
        [
            *MF,
            "query",
            "--metrics",
            ",".join(names),
            "--group-by",
            "metric_time__day",
            "--csv",
            str(out),
        ],
        cwd=ROOT / "dbt",
        env=env,
        check=True,
        capture_output=True,
    )
    with out.open() as f:
        return {
            row["metric_time__day"][:10]: {n: float(row[n] or 0) for n in names}
            for row in csv.DictReader(f)
        }


@pytest.mark.parametrize("view", PAIRS)
def test_evidence_metrics_match_the_semantic_layer(env, view, tmp_path):
    if not WAREHOUSE.exists():
        pytest.skip(f"no warehouse at {WAREHOUSE}: CI runs this after the replay (make ci-metrics)")
    pairs = PAIRS[view]
    evidence = _evidence(view, list(pairs))
    metricflow = _metricflow(list(pairs.values()), tmp_path)

    assert evidence, "no days in the warehouse: run make day and make dbt-build first"
    assert evidence.keys() == metricflow.keys()
    for day, values in evidence.items():
        for ev_name, mf_name in pairs.items():
            assert float(values[ev_name] or 0) == pytest.approx(metricflow[day][mf_name]), (
                f"{view}.{ev_name} on {day}"
            )
