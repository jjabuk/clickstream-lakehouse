from datetime import date

import pytest

from generator.rng import stream


def draw(*args, **kwargs):
    return stream(*args, **kwargs).random(5).tolist()


def test_same_seed_and_keys_give_identical_output():
    assert draw(42, "traffic", date(2026, 3, 1)) == draw(42, "traffic", date(2026, 3, 1))


def test_different_seed_keys_or_dates_give_different_output():
    base = draw(42, "traffic", date(2026, 3, 1))
    assert base != draw(43, "traffic", date(2026, 3, 1))
    assert base != draw(42, "catalog", date(2026, 3, 1))
    assert base != draw(42, "traffic", date(2026, 3, 2))


def test_streams_are_independent_of_each_other():
    """Using one stream must not change what another stream produces."""
    expected = draw(42, "catalog")
    noise = stream(42, "visitors")
    noise.random(1000)
    assert draw(42, "catalog") == expected


def test_negative_seed_or_key_is_rejected():
    with pytest.raises(ValueError):
        stream(-1)
    with pytest.raises(ValueError):
        stream(1, -5)
