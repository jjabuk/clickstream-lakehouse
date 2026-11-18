import json
from collections import Counter
from datetime import UTC, date, datetime, timedelta

import numpy as np
import pytest

from generator.batch import (
    build_batch,
    session_start_hours,
    simulate_day,
    to_jsonl,
)
from generator.catalog import build_catalog
from generator.config import GeneratorConfig
from generator.rng import stream
from generator.visitors import build_visitor_pool

# Small and dirty enough to exercise every path quickly.
CONFIG = GeneratorConfig(
    seed=5,
    n_visitors=300,
    sessions_per_day=250,
    bot_sessions_per_day=6,
    late_rate=0.2,
    duplicate_rate=0.1,
    utm_start=date(2026, 1, 4),
)
DAY0 = date(2026, 1, 2)


@pytest.fixture(scope="module")
def world():
    return build_catalog(CONFIG), build_visitor_pool(CONFIG)


def test_simulation_is_deterministic_and_seed_dependent(world):
    catalog, pool = world
    a = simulate_day(CONFIG, catalog, pool, DAY0)
    assert a == simulate_day(CONFIG, catalog, pool, DAY0)
    other = GeneratorConfig(seed=6, n_visitors=300, sessions_per_day=250)
    assert simulate_day(other, build_catalog(other), build_visitor_pool(other), DAY0) != a


def test_days_before_the_epoch_are_empty(world):
    catalog, pool = world
    sim = simulate_day(CONFIG, catalog, pool, CONFIG.epoch - timedelta(days=1))
    assert sim.events == [] and sim.orders == []


def test_batches_partition_all_deliveries_exactly_once(world):
    """The lookback is wide enough: no delivery is missed and none is duplicated."""
    catalog, pool = world
    first = CONFIG.epoch  # nothing is generated before it, so the set of deliveries is complete
    last = first + timedelta(days=6)
    days = [first + timedelta(days=i) for i in range((last - first).days + 1)]

    # Every delivery that arrives by `last` comes from a traffic day that is also <= `last`.
    expected = Counter(
        (r["event_id"], r["server_ts"])
        for d in days
        for r in simulate_day(CONFIG, catalog, pool, d).events
        if r["server_ts"].date() <= last
    )
    got = Counter()
    for d in days:
        batch = build_batch(CONFIG, catalog, pool, d)
        assert all(r["server_ts"].date() == d for r in batch)
        assert [r["server_ts"] for r in batch] == sorted(r["server_ts"] for r in batch)
        got.update((r["event_id"], r["server_ts"]) for r in batch)
    assert got == expected
    assert max(expected.values()) == 1  # a duplicate delivery has its own arrival time


def test_late_events_land_in_a_later_batch_than_they_happened(world):
    catalog, pool = world
    batch = build_batch(CONFIG, catalog, pool, DAY0 + timedelta(days=2))
    ages = [(r["server_ts"] - r["event_ts"]).total_seconds() for r in batch]
    # Skew can make this negative; true lateness is what dominates the upper tail.
    assert max(ages) > 3600
    assert max(ages) < (CONFIG.late_max_hours + 2) * 3600 + 2 * 3600 + 1


def test_duplicates_share_event_id_and_payload(world):
    catalog, pool = world
    events = simulate_day(CONFIG, catalog, pool, DAY0).events
    by_id: dict[str, list[dict]] = {}
    for r in events:
        by_id.setdefault(r["event_id"], []).append(r)
    dups = [rows for rows in by_id.values() if len(rows) > 1]
    assert dups
    for rows in dups:
        assert len(rows) == 2
        first, second = rows
        assert {k: v for k, v in first.items() if k != "server_ts"} == {
            k: v for k, v in second.items() if k != "server_ts"
        }
        assert second["server_ts"] > first["server_ts"]
    assert 0.05 < len(dups) / len(by_id) < 0.15  # duplicate_rate = 0.1


def test_utm_campaign_exists_only_from_its_start_day(world):
    catalog, pool = world
    before = simulate_day(CONFIG, catalog, pool, date(2026, 1, 3)).events
    after = simulate_day(CONFIG, catalog, pool, date(2026, 1, 5)).events
    # A session that starts just before midnight on the 3rd can carry events from the 4th.
    assert sum("utm_campaign" in r for r in before) < 0.02 * len(before)
    assert all("utm_campaign" in r for r in after)
    values = {r["utm_campaign"] for r in after} - {None}
    assert values and values <= set(CONFIG.utm_campaigns)


def test_bots_have_no_identity_and_humans_do(world):
    catalog, pool = world
    events = simulate_day(CONFIG, catalog, pool, DAY0).events
    anonymous = [r for r in events if r["cookie_id"] is None]
    assert anonymous and all(r["user_id"] is None for r in anonymous)
    assert all(r["cookie_id"] for r in events if r["cookie_id"] is not None)


def test_orders_have_stable_unique_ids_and_are_time_ordered(world):
    catalog, pool = world
    orders = simulate_day(CONFIG, catalog, pool, DAY0).orders
    assert orders
    assert [o.order_id for o in orders] == [orders[0].order_id + i for i in range(len(orders))]
    assert [o.created_at for o in orders] == sorted(o.created_at for o in orders)
    assert orders[0].order_id == (DAY0 - CONFIG.epoch).days * 1_000_000 + 1


def test_traffic_follows_the_diurnal_curve():
    rng = stream(1, "diurnal")
    hours = session_start_hours(rng, 40_000, amplitude=0.6, peak=19.0)
    assert len(hours) == 40_000 and hours.min() >= 0 and hours.max() < 24
    assert np.all(np.diff(hours) >= 0)
    near_peak = np.sum(np.abs(hours - 19.0) < 3)
    near_trough = np.sum(np.abs(hours - 7.0) < 3)
    # lambda(peak)/lambda(trough) = (1+a)/(1-a) = 4; the 6-hour windows give about 2.5x.
    assert near_peak > 2 * near_trough


def test_jsonl_is_one_utc_millisecond_object_per_line(world):
    catalog, pool = world
    rows = build_batch(CONFIG, catalog, pool, DAY0)[:50]
    lines = to_jsonl(rows).decode().splitlines()
    assert len(lines) == len(rows)
    parsed = [json.loads(line) for line in lines]
    assert parsed[0]["server_ts"].endswith("Z") and len(parsed[0]["server_ts"]) == 24
    assert datetime.fromisoformat(parsed[0]["event_ts"]).tzinfo is not None
    assert [p["event_id"] for p in parsed] == [r["event_id"] for r in rows]
    assert datetime.fromisoformat(parsed[0]["server_ts"]) == rows[0]["server_ts"].astimezone(UTC)
