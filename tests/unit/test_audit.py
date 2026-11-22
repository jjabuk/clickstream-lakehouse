from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest

from generator.faults import Fault, apply_fault
from ingest.audit import MIN_HISTORY_DAYS, audit_batch

T0 = datetime(2026, 2, 1, 10, tzinfo=UTC)


def rows(n: int) -> list[dict]:
    return [
        {
            "event_id": f"e{i}",
            "server_ts": T0 + timedelta(seconds=i),
            "event_type": "page_view" if i % 4 else "add_to_cart",
            "page_url": "/" if i % 4 else None,
        }
        for i in range(n)
    ]


def as_table(batch: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(batch)


def test_a_clean_batch_passes():
    result = audit_batch(as_table(rows(100)), history=[100, 90, 110])
    assert result.passed and result.rows == 100


def test_a_late_duplicate_event_is_dirt_not_a_violation():
    batch = rows(10)
    batch.append({**batch[0], "server_ts": T0 + timedelta(hours=3)})  # same event, later delivery
    assert audit_batch(as_table(batch), history=[]).passed


@pytest.mark.parametrize(
    ("fault", "message"),
    [
        (Fault.DUPLICATED, "repeat an (event_id, server_ts) delivery"),
        (Fault.BAD_EVENT_TYPE, "event_type outside"),
        (Fault.TRUNCATED, "below 50% of the median"),
    ],
)
def test_every_fault_is_caught(fault, message):
    result = audit_batch(as_table(apply_fault(rows(100), fault)), history=[100, 95, 105])
    assert not result.passed
    assert any(message in v for v in result.violations), result.violations


def test_volume_is_not_judged_without_enough_history():
    short = as_table(apply_fault(rows(100), Fault.TRUNCATED))
    assert audit_batch(short, history=[100] * (MIN_HISTORY_DAYS - 1)).passed


def test_an_empty_batch_fails():
    empty = as_table(rows(1)).slice(0, 0)
    assert "the batch is empty" in audit_batch(empty, history=[]).violations


def test_no_fault_leaves_the_batch_alone():
    batch = rows(5)
    assert apply_fault(batch, None) is batch
