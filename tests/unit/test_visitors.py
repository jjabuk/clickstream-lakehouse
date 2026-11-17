from datetime import date, timedelta

import numpy as np
import pytest

from generator.config import GeneratorConfig
from generator.visitors import AttributeChange, Visitor, build_visitor_pool

CONFIG = GeneratorConfig(seed=7)


@pytest.fixture(scope="module")
def pool():
    return build_visitor_pool(CONFIG)


def test_pool_is_deterministic(pool):
    again = build_visitor_pool(CONFIG)
    assert pool.visitors == again.visitors
    assert np.array_equal(pool.weights, again.weights)
    assert build_visitor_pool(GeneratorConfig(seed=8)).visitors != pool.visitors


def test_ids_are_unique(pool):
    cookies = [v.cookie_id for v in pool.visitors]
    user_ids = [v.user_id for v in pool.visitors if v.user_id is not None]
    assert len(set(cookies)) == len(cookies) == CONFIG.n_visitors
    assert user_ids == list(range(1, len(user_ids) + 1))


def test_registered_share_matches_config(pool):
    registered = sum(v.user_id is not None for v in pool.visitors) / len(pool.visitors)
    assert registered == pytest.approx(CONFIG.registered_share, abs=0.03)


def test_anonymous_visitors_have_no_plan_and_no_changes(pool):
    anonymous = [v for v in pool.visitors if v.user_id is None]
    assert anonymous
    assert all(v.base_plan is None and v.changes == () for v in anonymous)


def test_activity_is_heavy_tailed(pool):
    ordered = np.sort(pool.weights)[::-1]
    assert pool.weights.sum() == pytest.approx(1.0)
    assert ordered[: len(ordered) // 10].sum() > 0.25


def test_some_registered_users_change_attributes_within_horizon(pool):
    changes = [c for v in pool.visitors for c in v.changes]
    horizon_end = CONFIG.epoch + timedelta(days=CONFIG.attribute_horizon_days)
    assert {c.field for c in changes} == {"country", "plan"}
    assert all(CONFIG.epoch <= c.effective < horizon_end for c in changes)


def test_country_change_moves_to_a_different_country(pool):
    for v in pool.visitors:
        for c in v.changes:
            if c.field == "country":
                assert c.value != v.base_country


def test_attributes_on_applies_a_change_from_its_effective_day():
    visitor = Visitor(
        visitor_idx=0,
        cookie_id="c",
        user_id=1,
        mobile_share=0.4,
        base_country="PL",
        base_plan="free",
        changes=(
            AttributeChange(date(2026, 2, 1), "plan", "premium"),
            AttributeChange(date(2026, 3, 1), "country", "DE"),
        ),
    )
    assert visitor.attributes_on(date(2026, 1, 31)) == {"country": "PL", "plan": "free"}
    assert visitor.attributes_on(date(2026, 2, 1)) == {"country": "PL", "plan": "premium"}
    assert visitor.attributes_on(date(2026, 3, 1)) == {"country": "DE", "plan": "premium"}


def test_config_rejects_invalid_values():
    with pytest.raises(ValueError):
        GeneratorConfig(registered_share=1.5)
    with pytest.raises(ValueError):
        GeneratorConfig(n_products=5, n_categories=12)
    with pytest.raises(ValueError):
        GeneratorConfig(seed=-1)
