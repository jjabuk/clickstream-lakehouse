"""Check that Evidence's metrics equal dbt's, day by day (ADR 0023).

The metrics are defined in dbt's semantic layer (`dbt/models/marts/_marts.yml` and
`_metrics.yml`). Evidence has no MetricFlow connector, so `evidence/metrics/*.yaml` restates
them over the Postgres copy of the marts. Two definitions drift; this check catches it without
MetricFlow, whose CLI was removed (ADR 0024):

- every dbt metric is compiled to daily SQL on the DuckDB warehouse: a simple metric is its
  aggregation over its model, by the UTC date of the model's `agg_time_dimension`; a ratio is
  the ratio of two of them, joined on the day;
- every Evidence metric is compiled to daily SQL on the serving copy in Postgres, as Evidence
  runs it: its SQL over `base_sql`, by the metric file's `date`, references `{name}` resolved;
- every Evidence metric must have a dbt metric of the same name, since dbt is the definition,
  and the two must agree on every day.

It compares two definitions and two copies of the data at once: a publication that lost rows
fails it too. Days where a metric is null or zero are left out on both sides (a ratio with no
denominator, a day with no rows). A metric with no day left on either side is reported as
"no data" and does not fail: the four CI days are too early for an order to be returned.

Usage: python ci/check_metrics.py   (DBT_DUCKDB_PATH picks the warehouse; PLATFORM_POSTGRES_*
the serving database)
"""

import os
import re
import sys
from collections.abc import Mapping
from pathlib import Path

import duckdb
import psycopg
import yaml

ROOT = Path(__file__).resolve().parent.parent
MARTS_YML = ROOT / "dbt/models/marts/_marts.yml"
METRICS_YML = ROOT / "dbt/models/marts/_metrics.yml"
EVIDENCE_METRICS = ROOT / "evidence/metrics"
TOLERANCE = 1e-9

AGGREGATIONS = {
    "count": "count({})",
    "count_distinct": "count(distinct {})",
    "sum": "sum({})",
    "sum_boolean": "sum(case when {} then 1 else 0 end)",
    "average": "avg({})",
    "min": "min({})",
    "max": "max({})",
}
DIMENSION = re.compile(r"\{\{\s*Dimension\(\s*'(\w+)'\s*\)\s*\}\}")
REFERENCE = re.compile(r"\{(\w+)\}")

Daily = dict[str, float]  # ISO day -> value


# --- dbt --------------------------------------------------------------------------------------


def _filter_sql(where: str) -> str:
    """A MetricFlow filter as SQL: `{{ Dimension('entity__column') }}` is the model's column."""
    return DIMENSION.sub(lambda m: m.group(1).split("__", 1)[-1], where)


def dbt_metrics(marts: dict, metrics: dict) -> dict[str, dict]:
    """name -> the metric's definition, with `table` and `time` for the simple ones."""
    found = {}
    for model in marts["models"]:
        version = model.get("latest_version")
        table = f"{model['name']}_v{version}" if version else model["name"]
        for metric in model.get("metrics", []):
            found[metric["name"]] = {**metric, "table": table, "time": model["agg_time_dimension"]}
    for metric in metrics.get("metrics", []):
        found[metric["name"]] = metric
    return found


def _dbt_leaf(metric: dict) -> str:
    if metric["type"] != "simple":
        raise ValueError(f"{metric['name']}: not a simple metric")
    agg = AGGREGATIONS[metric["agg"]].format(metric["expr"])
    where = f" where {_filter_sql(metric['filter'])}" if metric.get("filter") else ""
    return (
        f"select cast({metric['time']} as date) as day, {agg} as v "
        f"from main.{metric['table']}{where} group by 1"
    )


def dbt_sql(metrics: Mapping[str, dict], name: str) -> str:
    """Daily SQL of one dbt metric: (day, value). Timestamps are cast to dates in UTC."""
    metric = metrics[name]
    if metric["type"] == "simple":
        return _dbt_leaf(metric)
    if metric["type"] != "ratio":
        raise ValueError(f"{name}: metric type {metric['type']} is not supported")
    num, den = metrics[metric["numerator"]], metrics[metric["denominator"]]
    return (
        f"with num as ({_dbt_leaf(num)}), den as ({_dbt_leaf(den)}) "
        "select coalesce(num.day, den.day) as day, "
        "coalesce(num.v, 0) * 1.0 / nullif(coalesce(den.v, 0), 0) as v "
        "from num full join den on num.day = den.day"
    )


# --- Evidence ---------------------------------------------------------------------------------


def evidence_metrics(files: Mapping[str, dict]) -> dict[str, dict]:
    """name -> (file's base_sql, date, the metric), across all metric files."""
    found = {}
    for path, spec in files.items():
        for name, metric in spec["metrics"].items():
            if name in found:
                raise ValueError(f"{name}: defined twice in Evidence ({path})")
            found[name] = {"base_sql": spec["base_sql"], "date": spec["date"], **metric}
    return found


def _leaves(metrics: Mapping[str, dict], name: str) -> list[str]:
    refs = REFERENCE.findall(metrics[name]["sql"])
    if not refs:
        return [name]
    return list(dict.fromkeys(leaf for ref in refs for leaf in _leaves(metrics, ref)))


def _expression(metrics: Mapping[str, dict], name: str) -> str:
    sql = metrics[name]["sql"]
    if not REFERENCE.search(sql):
        return f"coalesce({name}.v, 0)"
    return REFERENCE.sub(lambda m: f"({_expression(metrics, m.group(1))})", sql)


def evidence_sql(metrics: Mapping[str, dict], name: str) -> str:
    """Daily SQL of one Evidence metric, as Evidence runs it on the serving copy."""
    leaves = _leaves(metrics, name)
    ctes = []
    for leaf in leaves:
        m = metrics[leaf]
        where = f" where {m['filter']}" if m.get("filter") else ""
        ctes.append(
            f"{leaf} as (select cast({m['date']} as date) as day, {m['sql']} as v "
            f"from ({m['base_sql'].strip()}) as b{where} group by 1)"
        )
    days = " union ".join(f"select day from {leaf}" for leaf in leaves)
    joins = " ".join(f"left join {leaf} on {leaf}.day = days.day" for leaf in leaves)
    return (
        f"with {', '.join(ctes)}, days as ({days}) "
        f"select days.day, {_expression(metrics, name)} as v from days {joins}"
    )


# --- comparison -------------------------------------------------------------------------------


def _clean(rows) -> Daily:
    return {str(day): float(v) for day, v in rows if v is not None and float(v) != 0}


def differences(dbt: Daily, evidence: Daily) -> list[str]:
    """The days on which the two disagree, described."""
    problems = []
    for day in sorted(dbt.keys() | evidence.keys()):
        a, b = dbt.get(day), evidence.get(day)
        if a is None or b is None or abs(a - b) > TOLERANCE * max(abs(a), abs(b)):
            problems.append(f"{day}: dbt {a}, Evidence {b}")
    return problems


def verdict(dbt: Daily, evidence: Daily) -> str:
    """ok, DIFFERENT, or "no data": a metric with no rows on either side (the CI days have no
    order returns yet) is reported, not failed. One side empty is a difference."""
    if not dbt and not evidence:
        return "no data"
    return "DIFFERENT" if differences(dbt, evidence) else "ok"


def _dsn(env: Mapping[str, str]) -> str:
    return (
        f"host={env.get('PLATFORM_POSTGRES_HOST', 'localhost')} "
        f"port={env.get('PLATFORM_POSTGRES_PORT', '5433')} dbname=analytics "
        f"user={env['PLATFORM_POSTGRES_USER']} password={env['PLATFORM_POSTGRES_PASSWORD']}"
    )


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def main() -> None:
    env = os.environ
    dbt = dbt_metrics(_load(MARTS_YML), _load(METRICS_YML))
    evidence = evidence_metrics({p.name: _load(p) for p in sorted(EVIDENCE_METRICS.glob("*.yaml"))})
    unknown = sorted(evidence.keys() - dbt.keys())
    if unknown:
        sys.exit(f"Evidence metrics with no dbt metric of the same name: {', '.join(unknown)}")

    warehouse = env.get("DBT_DUCKDB_PATH", str(ROOT / "data/warehouse.duckdb"))
    failed = False
    with (
        duckdb.connect(warehouse, read_only=True) as duck,
        psycopg.connect(_dsn(env)) as pg,
    ):
        duck.execute("set TimeZone = 'UTC'")  # dbt's dates are UTC dates
        print(f"{'metric':<34} {'days':>5}  result")
        for name in sorted(evidence):
            ours = _clean(duck.sql(dbt_sql(dbt, name)).fetchall())
            theirs = _clean(pg.execute(evidence_sql(evidence, name)).fetchall())
            problems = differences(ours, theirs)
            print(f"{name:<34} {len(ours):>5}  {verdict(ours, theirs)}")
            for problem in problems[:5]:
                print(f"    {problem}")
            if len(problems) > 5:
                print(f"    ... and {len(problems) - 5} more days")
            failed |= bool(problems)
    only_dbt = sorted(dbt.keys() - evidence.keys())
    print(f"\ndbt metrics Evidence does not show (not compared): {', '.join(only_dbt)}")
    if failed:
        sys.exit("Evidence's metrics differ from dbt's.")
    print(f"\n{len(evidence)} Evidence metrics equal dbt's on every day they have data.")


if __name__ == "__main__":
    main()
