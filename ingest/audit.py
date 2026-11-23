"""The audit step of write-audit-publish (ADR 0008): the contract of one arrival day in bronze.

Bronze deliberately keeps dirt (duplicate deliveries, late rows, skewed clocks, bots), so the
audit does not judge data quality in general. It rejects a batch that is *broken*: something
dbt should never build on. A pure function of the batch and the volume history, so it is unit
tested without the stack.
"""

import statistics
from dataclasses import dataclass

import duckdb
import pyarrow as pa

EVENT_TYPES = ("page_view", "add_to_cart")
# A day with fewer rows than this share of the recent median is treated as a lost delivery.
# With the default generator the lowest day over 60 days is 0.80 of its 7-day median, so 0.5
# leaves room for normal variation and still catches a partial upload. The dbt volume_anomaly
# test on the source (a z-score, ADR 0024) is the statistical, warn-only counterpart.
MIN_VOLUME_SHARE = 0.5
# The volume check needs a few days of history before a median means anything.
MIN_HISTORY_DAYS = 3
HISTORY_DAYS = 7


@dataclass(frozen=True)
class AuditResult:
    rows: int
    violations: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.violations


def audit_batch(batch: pa.Table, history: list[int]) -> AuditResult:
    """Check one arrival day. `history` holds the row counts of the preceding arrival days."""
    con = duckdb.connect()
    con.register("batch", batch)
    types = ", ".join(f"'{t}'" for t in EVENT_TYPES)
    rows, duplicated, bad_types, pages_without_url = con.execute(
        f"""
        select
            count(*),
            count(*) - count(distinct (event_id, server_ts)),
            count(*) filter (where event_type is null or event_type not in ({types})),
            count(*) filter (where event_type = 'page_view' and page_url is null)
        from batch
        """
    ).fetchone()

    violations = []
    if rows == 0:
        violations.append("the batch is empty")
    if duplicated:
        # A duplicate *event* is normal dirt (same event_id, later server_ts); the same delivery
        # twice (same event_id and server_ts) means the file was loaded twice.
        violations.append(f"{duplicated} rows repeat an (event_id, server_ts) delivery")
    if bad_types:
        violations.append(f"{bad_types} rows have an event_type outside {list(EVENT_TYPES)}")
    if pages_without_url:
        violations.append(f"{pages_without_url} page views have no page_url")
    if len(history) >= MIN_HISTORY_DAYS:
        median = statistics.median(history)
        if rows < MIN_VOLUME_SHARE * median:
            violations.append(
                f"{rows} rows is below {MIN_VOLUME_SHARE:.0%} of the median of the last "
                f"{len(history)} days ({median:.0f})"
            )
    return AuditResult(rows, tuple(violations))
