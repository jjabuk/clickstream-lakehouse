from datetime import UTC, date, datetime, timedelta

import pytest

from generator.config import GeneratorConfig
from generator.orders import FINAL_STATUSES, Order, due_transitions, order_id_for, timeline

CONFIG = GeneratorConfig(seed=3)
T0 = datetime(2026, 3, 1, 12, 0, tzinfo=UTC)


def test_order_ids_are_blocks_per_day():
    assert order_id_for(CONFIG, CONFIG.epoch, 1) == 1
    assert order_id_for(CONFIG, CONFIG.epoch + timedelta(days=2), 7) == 2_000_007
    with pytest.raises(ValueError):
        order_id_for(CONFIG, CONFIG.epoch, 0)


def test_total_is_sum_of_lines():
    order = Order(1, T0, "c", None, ((1, 2, 500), (2, 1, 199)))
    assert order.total_cents == 1199


def test_timeline_is_deterministic_and_chronological():
    for order_id in range(1, 300):
        steps = timeline(CONFIG, order_id, T0)
        assert steps == timeline(CONFIG, order_id, T0)
        assert steps[0] == ("created", T0)
        times = [ts for _, ts in steps]
        assert times == sorted(times)
        assert len({name for name, _ in steps}) == len(steps)


def test_timeline_shapes_and_rates():
    finals = [timeline(CONFIG, i, T0)[-1][0] for i in range(1, 3001)]
    shares = {s: finals.count(s) / len(finals) for s in set(finals)}
    assert set(shares) <= {"delivered", "returned", "cancelled"}
    assert 0.04 < shares["returned"] + 0 < 0.10  # about 6% of the ~93% that get delivered
    assert 0.04 < shares["cancelled"] < 0.11  # 3% payment failures + ~4% cancelled later
    assert shares["delivered"] > 0.8


def test_due_transitions_plays_only_what_is_due_and_missing():
    steps = timeline(CONFIG, 1, T0)
    assert len(steps) >= 2
    # Nothing is due before the second step happens.
    assert due_transitions(steps, "created", steps[1][1]) == []
    # Just after it, exactly that step is due.
    assert due_transitions(steps, "created", steps[1][1] + timedelta(milliseconds=1)) == [steps[1]]
    # From the last status there is nothing left, whatever the horizon.
    assert due_transitions(steps, steps[-1][0], T0 + timedelta(days=365)) == []
    # Re-applying after a partial run continues where the order stands.
    assert due_transitions(steps, steps[1][0], T0 + timedelta(days=365)) == steps[2:]


def test_due_transitions_rejects_foreign_status():
    with pytest.raises(ValueError):
        due_transitions(timeline(CONFIG, 1, T0), "refunded", T0)


def test_final_statuses_end_the_timeline():
    for order_id in range(1, 500):
        steps = timeline(CONFIG, order_id, T0)
        for name, _ in steps[:-1]:
            assert name not in FINAL_STATUSES


def test_timeline_fits_the_lifecycle_window():
    from generator.orders import MAX_LIFECYCLE_DAYS

    for order_id in range(1, 2000):
        steps = timeline(CONFIG, order_id, T0)
        assert steps[-1][1] - T0 < timedelta(days=MAX_LIFECYCLE_DAYS)


def test_epoch_day_zero_id_is_not_negative():
    assert order_id_for(CONFIG, date(2026, 1, 1), 1) > 0
