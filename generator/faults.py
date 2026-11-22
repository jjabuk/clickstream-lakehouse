"""Faulty batches, to exercise write-audit-publish (ADR 0008).

The dirt in `dirt.py` is the normal state of a clickstream and bronze accepts it. A fault is a
broken delivery: it passes bronze's schema, so the write itself succeeds, but it breaks the
bronze contract that the audit checks. A fault only touches the landing file, never the shop's
Postgres, because it models a failure of the event pipeline, not of the shop.
"""

from enum import StrEnum

from generator.dirt import RawRow


class Fault(StrEnum):
    TRUNCATED = "truncated"  # the upload stopped early: most of the day is missing
    DUPLICATED = "duplicated"  # the same file was delivered twice
    BAD_EVENT_TYPE = "bad_event_type"  # a client release renamed an event type


TRUNCATED_SHARE = 0.2
BAD_EVENT_TYPE_EVERY = 20
BAD_EVENT_TYPE = "pageview"


def apply_fault(rows: list[RawRow], fault: Fault | None) -> list[RawRow]:
    """Return the batch as a broken delivery would look. Deterministic: no randomness."""
    match fault:
        case None:
            return rows
        case Fault.TRUNCATED:
            return rows[: int(len(rows) * TRUNCATED_SHARE)]
        case Fault.DUPLICATED:
            return rows + [dict(r) for r in rows]
        case Fault.BAD_EVENT_TYPE:
            # Every BAD_EVENT_TYPE_EVERY-th page view, counting page views only.
            out, page_views = [], 0
            for r in rows:
                if r["event_type"] == "page_view":
                    page_views += 1
                    if page_views % BAD_EVENT_TYPE_EVERY == 0:
                        r = {**r, "event_type": BAD_EVENT_TYPE}
                out.append(r)
            return out
