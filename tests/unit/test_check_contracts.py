"""The CI check for breaking contract changes (ADR 0025), on small hand-made manifests."""

from datetime import date

import pytest

from ci.check_contracts import breaking_changes

TODAY = date(2026, 11, 24)


def model(name, columns, version=None, enforced=True, deprecation_date=None):
    return {
        "resource_type": "model",
        "name": name,
        "version": version,
        "deprecation_date": deprecation_date,
        "config": {"contract": {"enforced": enforced}},
        "columns": {c: {"data_type": t} for c, t in columns.items()},
    }


def manifest(*models):
    return {"nodes": {f"model.p.{m['name']}.{m['version']}": m for m in models}}


SESSIONS = {"session_id": "varchar", "utm_campaign": "varchar", "page_views": "bigint"}


def test_adding_a_column_or_a_model_is_not_breaking():
    base = manifest(model("fct_sessions", SESSIONS))
    new = manifest(
        model("fct_sessions", {**SESSIONS, "device": "varchar"}),
        model("fct_orders", {"order_id": "bigint"}),
    )
    assert breaking_changes(base, new, TODAY) == []


@pytest.mark.parametrize(
    ("columns", "expected"),
    [
        ({"session_id": "varchar", "page_views": "bigint"}, "column utm_campaign removed"),
        (
            {**SESSIONS, "page_views": "integer"},
            "column page_views changed type bigint -> integer",
        ),
    ],
)
def test_removing_or_retyping_a_column_in_place_is_breaking(columns, expected):
    base = manifest(model("fct_sessions", SESSIONS))
    new = manifest(model("fct_sessions", columns))
    assert breaking_changes(base, new, TODAY) == [f"fct_sessions: {expected}"]


def test_dropping_the_contract_is_breaking():
    base = manifest(model("fct_sessions", SESSIONS))
    new = manifest(model("fct_sessions", SESSIONS, enforced=False))
    assert breaking_changes(base, new, TODAY) == [
        "fct_sessions: the contract is no longer enforced"
    ]


def test_a_rename_is_fine_as_a_new_version_that_keeps_the_old_one():
    renamed = {"session_id": "varchar", "campaign": "varchar", "page_views": "bigint"}
    base = manifest(model("fct_sessions", SESSIONS))
    new = manifest(
        model("fct_sessions", renamed, version=2),
        model("fct_sessions", SESSIONS, version=1, deprecation_date="2027-01-31"),
    )
    assert breaking_changes(base, new, TODAY) == []


def test_a_rename_without_a_version_that_keeps_the_old_columns_is_breaking():
    renamed = {"session_id": "varchar", "campaign": "varchar", "page_views": "bigint"}
    base = manifest(model("fct_sessions", SESSIONS))
    new = manifest(model("fct_sessions", renamed, version=2))
    assert breaking_changes(base, new, TODAY) == [
        "fct_sessions: removed, and no version of fct_sessions keeps its columns"
    ]


def test_an_old_version_can_go_only_after_its_deprecation_date():
    v2 = {"session_id": "varchar", "campaign": "varchar"}
    v1 = {"session_id": "varchar", "utm_campaign": "varchar"}
    base = manifest(
        model("fct_sessions", v2, version=2),
        model("fct_sessions", v1, version=1, deprecation_date="2027-01-31"),
    )
    new = manifest(model("fct_sessions", v2, version=2))
    assert breaking_changes(base, new, TODAY) == [
        "fct_sessions v1: removed, and no version of fct_sessions keeps its columns"
    ]
    assert breaking_changes(base, new, date(2027, 2, 1)) == []
