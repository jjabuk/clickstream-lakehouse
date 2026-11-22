"""Week 3 acceptance for write-audit-publish (ADR 0008): a broken batch stays on its branch.

Runs against the real stack in throwaway resources, like test_day.py.
"""

from datetime import date, timedelta

import pytest

from generator.config import GeneratorConfig
from generator.day import run_day
from generator.faults import Fault
from ingest import landing
from ingest.bronze import TABLE_NAME, AuditFailed, load_day, wap_branch

pytestmark = pytest.mark.integration

DAY = date(2026, 1, 15)
# Few bot sessions: each one is ~150 events, so with several the daily volume of a small world
# swings enough to trip the volume check (worst day 0.69 of the 7-day median with one bot).
CONFIG = GeneratorConfig(seed=11, n_visitors=300, sessions_per_day=300, bot_sessions_per_day=1)


def load(env, catalog, namespace, sandbox, day, fault=None):
    prefix, schema = sandbox
    report = run_day(CONFIG, day, env, prefix=prefix, pg_schema=schema, fault=fault)
    return load_day(catalog, landing.filesystem(env), report.landing_path, day, namespace)


def rows_on_main(duck, namespace, day):
    duck.execute("SET TimeZone = 'UTC'")
    return duck.execute(
        f"SELECT count(*) FROM lk.{namespace}.{TABLE_NAME} WHERE CAST(server_ts AS DATE) = ?",
        [day],
    ).fetchone()[0]


def test_a_truncated_batch_stays_on_its_branch_until_a_good_one_replaces_it(
    env, catalog, namespace, duck, sandbox
):
    for offset in range(3):  # enough history for the volume check
        load(env, catalog, namespace, sandbox, DAY + timedelta(days=offset))
    bad_day = DAY + timedelta(days=3)
    main_before = catalog.load_table(f"{namespace}.{TABLE_NAME}").current_snapshot().snapshot_id

    with pytest.raises(AuditFailed) as failure:
        load(env, catalog, namespace, sandbox, bad_day, fault=Fault.TRUNCATED)
    assert any("below 50% of the median" in v for v in failure.value.result.violations)

    table = catalog.load_table(f"{namespace}.{TABLE_NAME}")
    assert table.current_snapshot().snapshot_id == main_before  # main did not move
    assert wap_branch(bad_day) in table.refs()  # the batch is kept for inspection
    assert rows_on_main(duck, namespace, bad_day) == 0  # readers (dbt) never see it

    good = load(env, catalog, namespace, sandbox, bad_day)  # the fixed delivery
    table = catalog.load_table(f"{namespace}.{TABLE_NAME}")
    assert wap_branch(bad_day) not in table.refs()
    assert table.current_snapshot().snapshot_id == good.snapshot_id
    assert rows_on_main(duck, namespace, bad_day) == good.rows


@pytest.mark.parametrize("fault", [Fault.DUPLICATED, Fault.BAD_EVENT_TYPE])
def test_contract_violations_are_caught_without_history(env, catalog, namespace, sandbox, fault):
    with pytest.raises(AuditFailed):
        load(env, catalog, namespace, sandbox, DAY, fault=fault)
    # Checked with PyIceberg: main holds only the empty bootstrap commit here, which DuckDB
    # 2.0 cannot read (ADR 0008).
    assert catalog.load_table(f"{namespace}.{TABLE_NAME}").scan().to_arrow().num_rows == 0
