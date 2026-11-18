"""Orders and their status lifecycle.

Every order has a status timeline that is a pure function of (seed, order_id, created_at):

    created -> paid -> shipped -> delivered [-> returned]
    created -> cancelled              (payment failed)
    created -> paid -> cancelled      (cancelled before shipping)

The timeline is computed once and applied to Postgres as the days pass, one UPDATE per status.
Because nothing is stored besides the order row, any day can be (re)applied on its own: the
applier looks at the current status and plays only the transitions that are due and still missing.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta

from generator.config import GeneratorConfig
from generator.rng import stream

STATUSES = ("created", "paid", "shipped", "delivered", "returned", "cancelled")
FINAL_STATUSES = frozenset({"returned", "cancelled"})
# After this many days no further transition can be scheduled (delivery + return window).
MAX_LIFECYCLE_DAYS = 30

PAYMENT_FAILED_PROB = 0.03
CANCEL_BEFORE_SHIPPING_PROB = 0.04
RETURN_PROB = 0.06


@dataclass(frozen=True)
class Order:
    order_id: int
    created_at: datetime
    cookie_id: str
    user_id: int | None
    items: tuple[tuple[int, int, int], ...]  # (product_id, quantity, unit_price_cents)

    @property
    def total_cents(self) -> int:
        return sum(qty * price for _, qty, price in self.items)


def order_id_for(config: GeneratorConfig, day: date, seq: int) -> int:
    """Deterministic id: one block of a million per simulated day, `seq` counts from 1."""
    if not 1 <= seq < 1_000_000:
        raise ValueError("at most 999999 orders per day")
    return (day - config.epoch).days * 1_000_000 + seq


def timeline(
    config: GeneratorConfig, order_id: int, created_at: datetime
) -> list[tuple[str, datetime]]:
    """All (status, time) steps of the order, the first being `created`."""
    rng = stream(config.seed, "order", order_id)
    steps = [("created", created_at)]

    def after(prev: datetime, lo_s: float, hi_s: float) -> datetime:
        ts = prev + timedelta(seconds=float(rng.uniform(lo_s, hi_s)))
        return ts.replace(microsecond=ts.microsecond // 1000 * 1000)

    if rng.random() < PAYMENT_FAILED_PROB:
        steps.append(("cancelled", after(created_at, 600, 1800)))
        return steps
    paid_at = after(created_at, 60, 1800)
    steps.append(("paid", paid_at))
    if rng.random() < CANCEL_BEFORE_SHIPPING_PROB:
        steps.append(("cancelled", after(paid_at, 600, 2 * 3600)))
        return steps
    shipped_at = after(paid_at, 2 * 3600, 48 * 3600)
    steps.append(("shipped", shipped_at))
    delivered_at = after(shipped_at, 86400, 6 * 86400)
    steps.append(("delivered", delivered_at))
    if rng.random() < RETURN_PROB:
        steps.append(("returned", after(delivered_at, 86400, 14 * 86400)))
    return steps


def due_transitions(
    steps: list[tuple[str, datetime]], current_status: str, until: datetime
) -> list[tuple[str, datetime]]:
    """Steps after `current_status` whose time is before `until`, in order.

    Transitions are strictly sequential, so stopping at the first one that is not yet due is
    enough; later steps are later in time as well.
    """
    names = [name for name, _ in steps]
    if current_status not in names:
        raise ValueError(f"status {current_status!r} is not on this order's timeline {names}")
    due = []
    for name, ts in steps[names.index(current_status) + 1 :]:
        if ts >= until:
            break
        due.append((name, ts))
    return due
