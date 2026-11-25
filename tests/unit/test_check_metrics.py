"""The metrics check (ADR 0023): dbt's and Evidence's definitions compiled to daily SQL, run on
small in-memory tables. Postgres and DuckDB share the SQL used here."""

import duckdb
import pytest

from ci.check_metrics import (
    _clean,
    dbt_metrics,
    dbt_sql,
    differences,
    evidence_metrics,
    evidence_sql,
)

MARTS = {
    "models": [
        {
            "name": "fct_sessions",
            "latest_version": 2,
            "agg_time_dimension": "started_at",
            "metrics": [
                {"name": "sessions", "type": "simple", "agg": "count", "expr": "session_id"},
                {"name": "bounces", "type": "simple", "agg": "sum_boolean", "expr": "is_bounce"},
                {"name": "visitors", "type": "simple", "agg": "count_distinct", "expr": "cookie"},
            ],
        },
        {
            "name": "fct_traffic",
            "agg_time_dimension": "day",
            "metrics": [
                {"name": "events", "type": "simple", "agg": "sum", "expr": "events"},
                {
                    "name": "bot_events",
                    "type": "simple",
                    "agg": "sum",
                    "expr": "events",
                    "filter": "{{ Dimension('traffic_day__is_bot') }}",
                },
            ],
        },
    ]
}
RATIOS = {
    "metrics": [
        {"name": "bounce_rate", "type": "ratio", "numerator": "bounces", "denominator": "sessions"},
        {"name": "bot_share", "type": "ratio", "numerator": "bot_events", "denominator": "events"},
    ]
}
EVIDENCE = {
    "sessions.yaml": {
        "base_sql": "select session_id, is_bounce::int as bounced, "
        "cast(started_at as date) as day from main.fct_sessions_v2",
        "date": "day",
        "metrics": {
            "sessions": {"sql": "count(*)"},
            "bounces": {"sql": "sum(bounced)"},
            "bounce_rate": {"sql": "{bounces} * 1.0 / nullif({sessions}, 0)"},
        },
    },
    "traffic.yaml": {
        "base_sql": "select day, events, case when is_bot then 'bot' else 'human' end as visitor "
        "from main.fct_traffic",
        "date": "day",
        "metrics": {
            "events": {"sql": "sum(events)"},
            "bot_events": {"sql": "sum(events)", "filter": "visitor = 'bot'"},
            "bot_share": {"sql": "{bot_events} * 1.0 / nullif({events}, 0)"},
        },
    },
}


@pytest.fixture
def warehouse():
    con = duckdb.connect()
    con.execute("set TimeZone = 'UTC'")
    con.execute("""
        create table fct_sessions_v2 as select * from (values
            ('s1', 'c1', true,  timestamptz '2026-03-01 10:00:00+00'),
            ('s2', 'c1', false, timestamptz '2026-03-01 23:30:00+00'),
            ('s3', 'c2', true,  timestamptz '2026-03-02 00:10:00+00')
        ) as t(session_id, cookie, is_bounce, started_at)
    """)
    con.execute("""
        create table fct_traffic as select * from (values
            (date '2026-03-01', false, 90), (date '2026-03-01', true, 10),
            (date '2026-03-02', false, 50)
        ) as t(day, is_bot, events)
    """)
    yield con
    con.close()


def run(con, sql):
    return _clean(con.sql(sql).fetchall())


def test_simple_metrics_by_utc_day(warehouse):
    metrics = dbt_metrics(MARTS, RATIOS)
    assert metrics["sessions"]["table"] == "fct_sessions_v2"  # the latest version
    assert run(warehouse, dbt_sql(metrics, "sessions")) == {"2026-03-01": 2, "2026-03-02": 1}
    assert run(warehouse, dbt_sql(metrics, "bounces")) == {"2026-03-01": 1, "2026-03-02": 1}
    assert run(warehouse, dbt_sql(metrics, "visitors")) == {"2026-03-01": 1, "2026-03-02": 1}


def test_a_filter_on_a_dimension_becomes_the_models_column(warehouse):
    metrics = dbt_metrics(MARTS, RATIOS)
    assert "where is_bot" in dbt_sql(metrics, "bot_events")
    assert run(warehouse, dbt_sql(metrics, "bot_events")) == {"2026-03-01": 10}


def test_a_ratio_is_a_ratio_of_sums_per_day(warehouse):
    metrics = dbt_metrics(MARTS, RATIOS)
    assert run(warehouse, dbt_sql(metrics, "bounce_rate")) == {"2026-03-01": 0.5, "2026-03-02": 1}
    # No bot events on 2026-03-02: a zero share, left out like a missing day.
    assert run(warehouse, dbt_sql(metrics, "bot_share")) == {"2026-03-01": 0.1}


@pytest.mark.parametrize("name", ["sessions", "bounce_rate", "events", "bot_events", "bot_share"])
def test_evidence_resolves_references_and_filters_to_the_same_numbers(warehouse, name):
    dbt = dbt_metrics(MARTS, RATIOS)
    evidence = evidence_metrics(EVIDENCE)
    ours = run(warehouse, dbt_sql(dbt, name))
    theirs = run(warehouse, evidence_sql(evidence, name))
    assert ours and differences(ours, theirs) == []


def test_a_different_definition_is_reported_by_day(warehouse):
    evidence = evidence_metrics(EVIDENCE)
    evidence["bounce_rate"] = {**evidence["bounce_rate"], "sql": "{bounces} * 1.0 / 4"}
    dbt = run(warehouse, dbt_sql(dbt_metrics(MARTS, RATIOS), "bounce_rate"))
    found = differences(dbt, run(warehouse, evidence_sql(evidence, "bounce_rate")))
    assert found == ["2026-03-01: dbt 0.5, Evidence 0.25", "2026-03-02: dbt 1.0, Evidence 0.25"]


def test_a_day_missing_on_one_side_is_a_difference():
    assert differences({"2026-03-01": 1.0}, {}) == ["2026-03-01: dbt 1.0, Evidence None"]


def test_a_metric_defined_twice_in_evidence_is_an_error():
    with pytest.raises(ValueError, match="defined twice"):
        evidence_metrics({"a.yaml": EVIDENCE["sessions.yaml"], "b.yaml": EVIDENCE["sessions.yaml"]})
