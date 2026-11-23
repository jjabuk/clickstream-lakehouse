"""The decisions behind the Iceberg operations (ADR 0016), on hand-made metadata."""

from datetime import date

import pytest

from ingest.maintenance import (
    DataFile,
    HistoryEntry,
    fragmented_days,
    snapshot_before_later_days,
    snapshots_to_expire,
)

D1, D2, D3 = date(2026, 2, 1), date(2026, 2, 2), date(2026, 2, 3)


def file(day, partition, spec_id=0, n=0):
    return DataFile(f"s3://f/{day}/{partition}/{n}", spec_id, partition, day, 10, 1000)


def test_a_day_with_two_files_in_one_partition_is_fragmented():
    files = [file(D1, "d1"), file(D2, "d2", n=0), file(D2, "d2", n=1)]
    assert fragmented_days(files) == [D2]


def test_one_file_per_hour_is_not_fragmented():
    files = [file(D1, f"h{h}", spec_id=1) for h in range(24)]
    assert fragmented_days(files) == []


def test_files_of_two_specs_in_one_day_are_not_fragmented():
    # The day was written under the day spec and partly rewritten under the hour spec.
    assert fragmented_days([file(D1, "d1", spec_id=0), file(D1, "h3", spec_id=1)]) == []


def history(*entries):
    return [HistoryEntry(sid, day, maintenance) for sid, day, maintenance in entries]


def test_time_travel_stops_before_the_first_later_day():
    h = history((1, None, False), (2, D1, False), (3, None, False), (4, D2, False), (5, D3, False))
    assert snapshot_before_later_days(h, D1) == 3  # 3 is a delete of a re-run
    assert snapshot_before_later_days(h, D2) == 4
    assert snapshot_before_later_days(h, D3) == 5


def test_maintenance_commits_are_neither_a_later_day_nor_a_published_state():
    # 3 and 4 compact D2 after D2 arrived: a delete, then the append that writes D2 again.
    h = history((1, D1, False), (2, D2, False), (3, None, True), (4, D2, True), (5, D3, False))
    assert snapshot_before_later_days(h, D1) == 1
    assert snapshot_before_later_days(h, D2) == 2


def test_no_time_travel_once_the_load_of_the_day_expired():
    # Only the compaction of D2 and the load of D3 are left in the history.
    h = history((3, None, True), (4, D2, True), (5, D3, False))
    assert snapshot_before_later_days(h, D2) is None


def test_expiry_keeps_the_newest_of_main_and_every_ref_head():
    main = [9, 8, 7, 6, 5]  # newest first
    # 4 is the head of a failed WAP branch; 3 was on a branch that was replaced.
    assert snapshots_to_expire([3, 4, 5, 6, 7, 8, 9], main, {9, 4}, keep=2) == [3, 5, 6, 7]


def test_expiry_never_drops_the_current_snapshot():
    with pytest.raises(ValueError):
        snapshots_to_expire([1, 2], [2, 1], {2}, keep=0)
