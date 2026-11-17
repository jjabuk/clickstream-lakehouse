"""Visitor pool: anonymous cookies and registered users whose attributes change over time.

Attribute changes (country, plan) are a deterministic schedule derived from the seed, so the
value on any day can be computed without replaying history. They are the raw material for SCD2.
"""

import uuid
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np

from generator.config import GeneratorConfig
from generator.rng import stream

COUNTRIES = ("PL", "DE", "GB", "FR", "US", "NL", "ES", "IT", "SE", "CZ")
COUNTRY_WEIGHTS = (0.30, 0.16, 0.12, 0.09, 0.12, 0.05, 0.05, 0.04, 0.03, 0.04)
PLANS = ("free", "premium")


@dataclass(frozen=True)
class AttributeChange:
    effective: date  # the new value applies from 00:00 UTC of this day
    field: str  # "country" or "plan"
    value: str


@dataclass(frozen=True)
class Visitor:
    visitor_idx: int
    cookie_id: str
    user_id: int | None  # None for anonymous visitors
    mobile_share: float  # probability that a session of this visitor is on mobile
    base_country: str
    base_plan: str | None  # None for anonymous visitors
    changes: tuple[AttributeChange, ...] = ()

    def attributes_on(self, day: date) -> dict[str, str | None]:
        """Country and plan in effect on `day` (changes apply from their effective day)."""
        attrs: dict[str, str | None] = {"country": self.base_country, "plan": self.base_plan}
        for change in self.changes:  # sorted by effective date
            if change.effective <= day:
                attrs[change.field] = change.value
        return attrs


@dataclass(frozen=True)
class VisitorPool:
    visitors: tuple[Visitor, ...]
    weights: np.ndarray  # relative activity (heavy-tailed), sums to 1


def build_visitor_pool(config: GeneratorConfig) -> VisitorPool:
    rng = stream(config.seed, "visitors")
    n = config.n_visitors

    registered = rng.random(n) < config.registered_share
    countries = rng.choice(COUNTRIES, size=n, p=COUNTRY_WEIGHTS)
    mobile_share = rng.beta(2, 3, size=n)
    activity = rng.lognormal(mean=0.0, sigma=1.0, size=n)
    weights = activity / activity.sum()

    visitors = []
    next_user_id = 1
    for i in range(n):
        cookie_id = str(uuid.UUID(bytes=rng.bytes(16), version=4))
        country = str(countries[i])
        if not registered[i]:
            visitors.append(Visitor(i, cookie_id, None, float(mobile_share[i]), country, None))
            continue
        plan = "premium" if rng.random() < config.premium_share else "free"
        changes = _schedule_changes(rng, config, country, plan)
        visitors.append(
            Visitor(i, cookie_id, next_user_id, float(mobile_share[i]), country, plan, changes)
        )
        next_user_id += 1
    return VisitorPool(tuple(visitors), weights)


def _schedule_changes(
    rng: np.random.Generator, config: GeneratorConfig, country: str, plan: str
) -> tuple[AttributeChange, ...]:
    changes: list[AttributeChange] = []

    def random_day() -> date:
        return config.epoch + timedelta(days=int(rng.integers(0, config.attribute_horizon_days)))

    if rng.random() < config.country_change_prob:
        others = [c for c in COUNTRIES if c != country]
        changes.append(AttributeChange(random_day(), "country", str(rng.choice(others))))

    plan_prob = config.plan_change_prob_free if plan == "free" else config.plan_change_prob_premium
    if rng.random() < plan_prob:
        new_plan = "premium" if plan == "free" else "free"
        changes.append(AttributeChange(random_day(), "plan", new_plan))

    return tuple(sorted(changes, key=lambda c: c.effective))
