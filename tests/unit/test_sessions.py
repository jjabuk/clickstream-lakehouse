from datetime import UTC, datetime

import numpy as np
import pytest

from generator.batch import hot_products
from generator.catalog import build_catalog
from generator.config import GeneratorConfig
from generator.rng import stream
from generator.sessions import (
    TRANSITIONS,
    CatalogSampler,
    simulate_bot_session,
    simulate_human_session,
)
from generator.visitors import build_visitor_pool

CONFIG = GeneratorConfig(seed=11)
START = datetime(2026, 3, 1, 10, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def world():
    catalog = build_catalog(CONFIG)
    return catalog, CatalogSampler(catalog), build_visitor_pool(CONFIG)


def human_sessions(world, n=600):
    _, sampler, pool = world
    return [
        simulate_human_session(
            stream(CONFIG.seed, "t", i), CONFIG, sampler, pool.visitors[i % 50], START, "PL"
        )
        for i in range(n)
    ]


def test_transition_rows_sum_to_one():
    for state, row in TRANSITIONS.items():
        assert sum(p for _, p in row) == pytest.approx(1.0), state


def test_human_sessions_are_ordered_and_reference_the_catalog(world):
    catalog = world[0]
    valid_products = {p.product_id for p in catalog.products}
    for s in human_sessions(world):
        times = [e.ts for e in s.events]
        assert times == sorted(times)
        assert all(e.ts >= START for e in s.events)
        assert len({e.event_id for e in s.events}) == len(s.events)
        for e in s.events:
            assert e.product_id is None or e.product_id in valid_products
            assert (e.event_type == "add_to_cart") == (e.quantity is not None)


def test_orders_match_what_was_put_in_the_cart(world):
    sessions = human_sessions(world, 1500)
    with_order = [s for s in sessions if s.order is not None]
    assert with_order, "expected some conversions in 1500 sessions"
    for s in with_order:
        added: dict[int, int] = {}
        for e in s.events:
            if e.event_type == "add_to_cart":
                added[e.product_id] = added.get(e.product_id, 0) + e.quantity
        assert {pid: qty for pid, qty, _ in s.order.items} == added
        assert s.events[-1].page_type == "checkout"
        assert s.order.created_at > s.events[-1].ts
        assert s.order.cookie_id == s.cookie_id
    # Conversion is a few percent, not zero and not half of the traffic.
    assert 0.01 < len(with_order) / len(sessions) < 0.12


def test_funnel_is_monotone(world):
    sessions = human_sessions(world, 1500)
    reached = {
        page: sum(any(e.page_type == page for e in s.events) for s in sessions)
        for page in ("product", "cart", "checkout")
    }
    assert reached["product"] > reached["cart"] > reached["checkout"] > 0


def test_login_requires_a_registered_visitor(world):
    _, sampler, pool = world
    anonymous = next(v for v in pool.visitors if v.user_id is None)
    s = simulate_human_session(stream(1, "a"), CONFIG, sampler, anonymous, START, "PL")
    assert s.user_id is None and s.cookie_id == anonymous.cookie_id


def test_bot_sessions_are_fast_cookieless_and_concentrated(world):
    catalog = world[0]
    hot = hot_products(CONFIG, catalog)
    assert len(set(hot.tolist())) == CONFIG.n_hot_products
    s = simulate_bot_session(stream(1, "b"), CONFIG, hot, START, "US")
    assert s.cookie_id is None and s.user_id is None and s.order is None
    gaps = np.diff([e.ts.timestamp() for e in s.events])
    assert np.median(gaps) < 1.0
    assert {e.product_id for e in s.events} <= set(hot.tolist())
    assert len(s.events) > 20
