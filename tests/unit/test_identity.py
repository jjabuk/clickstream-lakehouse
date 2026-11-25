from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta

import pytest

from generator.batch import simulate_day
from generator.catalog import build_catalog
from generator.config import GeneratorConfig
from generator.dirt import apply_dirt
from generator.identity import assign_identity
from generator.rng import stream
from generator.sessions import CatalogSampler, simulate_human_session
from generator.visitors import build_visitor_pool

CONFIG = GeneratorConfig(seed=5)
BEFORE = CONFIG.identity_start - timedelta(days=1)
START = datetime.combine(CONFIG.identity_start, datetime.min.time(), tzinfo=UTC)


@pytest.fixture(scope="module")
def world():
    return build_catalog(CONFIG), build_visitor_pool(CONFIG)


def _cookies_by_user(events):
    cookies = defaultdict(set)
    for e in events:
        if e["user_id"] is not None:
            cookies[e["user_id"]].add(e["cookie_id"])
    return cookies


def test_every_registered_user_has_a_cookie_per_device(world):
    _, pool = world
    registered = [v for v in pool.visitors if v.user_id is not None]
    assert set(pool.device_cookies) == {v.visitor_idx for v in registered}
    for v in registered:
        cookies = pool.device_cookies[v.visitor_idx]
        assert set(cookies) == {"desktop", "mobile"}
        assert cookies["desktop"] != cookies["mobile"]


def test_a_shared_desktop_belongs_to_two_users_at_most(world):
    _, pool = world
    owners = Counter(c["desktop"] for c in pool.device_cookies.values())
    assert 1 < max(owners.values()) <= 2


def test_before_identity_start_a_user_has_one_cookie(world):
    catalog, pool = world
    events = simulate_day(CONFIG, catalog, pool, BEFORE).events
    cookie_of = {v.user_id: v.cookie_id for v in pool.visitors}
    for user_id, cookies in _cookies_by_user(events).items():
        assert cookies == {cookie_of[user_id]}


def test_from_identity_start_users_switch_devices(world):
    catalog, pool = world
    events = simulate_day(CONFIG, catalog, pool, CONFIG.identity_start).events
    assert any(len(c) > 1 for c in _cookies_by_user(events).values())


def _logged_in_sessions(world, n=400):
    catalog, pool = world
    sampler = CatalogSampler(catalog)
    users = [v for v in pool.visitors if v.user_id is not None]
    for i in range(n):
        visitor = users[i % len(users)]
        session = simulate_human_session(
            stream(CONFIG.seed, "t", i), CONFIG, sampler, visitor, START, "PL"
        )
        if session.user_id is None:
            continue
        cookies = pool.device_cookies[visitor.visitor_idx]
        yield assign_identity(stream(CONFIG.seed, "i", i), CONFIG, cookies, session), cookies


def test_a_login_can_come_mid_session_and_the_views_before_it_are_anonymous(world):
    mid_session = 0
    for session, cookies in _logged_in_sessions(world):
        assert session.cookie_id == cookies[session.device]
        rows = apply_dirt(stream(CONFIG.seed, "d", 0), CONFIG, session)
        by_event = {r["event_id"]: r["user_id"] for r in rows}
        for i, event in enumerate(session.events):
            expected = session.user_id if i >= session.logged_in_from else None
            assert by_event[event.event_id] == expected
        mid_session += session.logged_in_from > 0
    assert mid_session > 0


def test_an_order_is_placed_logged_in_on_the_sessions_cookie(world):
    catalog, pool = world
    sim = simulate_day(CONFIG, catalog, pool, CONFIG.identity_start)
    checkouts = {(e["cookie_id"], e["user_id"]) for e in sim.events if e["page_type"] == "checkout"}
    for order in sim.orders:
        assert (order.cookie_id, order.user_id) in checkouts


def test_config_rejects_an_invalid_identity_share():
    with pytest.raises(ValueError):
        GeneratorConfig(shared_device_share=1.2)
