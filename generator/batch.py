"""One simulated day, and the daily batch that the ingestion receives.

Two different days matter and they are kept apart:

- the *traffic day*: sessions that start on it. `simulate_day` produces their events and orders;
- the *arrival day*: the day an event reaches the server (`server_ts`). A batch holds one arrival
  day. Late events and delayed duplicates of earlier traffic days land in it.

Each traffic day is generated from independent random streams, so the batch for an arrival day
is rebuilt on its own by simulating the few traffic days that can still deliver into it.
"""

import json
import math
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

import numpy as np

from generator.catalog import Catalog
from generator.config import GeneratorConfig
from generator.dirt import RawRow, apply_dirt
from generator.identity import assign_identity
from generator.orders import Order, order_id_for
from generator.rng import stream
from generator.sessions import CatalogSampler, simulate_bot_session, simulate_human_session
from generator.visitors import COUNTRIES, VisitorPool


def lookback_days(config: GeneratorConfig) -> int:
    """How many traffic days before an arrival day can still deliver into it.

    A late event arrives up to `late_max_hours` after it happened, and a session or a retried
    duplicate can add a few hours on top, hence one extra day.
    """
    return math.ceil(config.late_max_hours / 24) + 1


@dataclass(frozen=True)
class DaySimulation:
    day: date
    events: list[RawRow]  # every delivery, dirt included; may arrive on later days
    orders: list[Order]


def _midnight(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=UTC)


def session_start_hours(
    rng: np.random.Generator, n: int, amplitude: float, peak: float
) -> np.ndarray:
    """`n` start times (hours in [0, 24)) from lambda(t) ~ 1 + a*sin(2*pi*(t - peak + 6)/24).

    Given the number of sessions, the start times of a Poisson process are i.i.d. with density
    proportional to lambda(t). Rejection sampling against the maximum (1 + a) is exact.
    """
    out: list[float] = []
    while len(out) < n:
        u = rng.uniform(0, 24, size=2 * (n - len(out)) + 8)
        density = (1 + amplitude * np.sin(2 * np.pi * (u - peak + 6) / 24)) / (1 + amplitude)
        out.extend(u[rng.random(len(u)) < density][: n - len(out)])
    return np.sort(np.array(out))


def hot_products(config: GeneratorConfig, catalog: Catalog) -> np.ndarray:
    """The products bots hammer. Fixed per seed, so the hot keys are stable across days."""
    ids = np.array([p.product_id for p in catalog.products])
    return stream(config.seed, "hot_products").choice(
        ids, size=config.n_hot_products, replace=False
    )


def simulate_day(
    config: GeneratorConfig, catalog: Catalog, pool: VisitorPool, day: date
) -> DaySimulation:
    if day < config.epoch:
        return DaySimulation(day, [], [])
    sampler = CatalogSampler(catalog)
    hot = hot_products(config, catalog)
    midnight = _midnight(day)

    arrivals = stream(config.seed, "arrivals", day)
    n_human = int(arrivals.poisson(config.sessions_per_day))
    n_bots = int(arrivals.poisson(config.bot_sessions_per_day))
    hours = session_start_hours(arrivals, n_human, config.diurnal_amplitude, config.peak_hour_utc)
    visitor_idx = arrivals.choice(len(pool.visitors), size=n_human, p=pool.weights)
    bot_hours = np.sort(arrivals.uniform(0, 24, size=n_bots))

    events: list[RawRow] = []
    intents = []
    for i in range(n_human):
        rng = stream(config.seed, "session", day, i)
        visitor = pool.visitors[int(visitor_idx[i])]
        start = midnight + timedelta(hours=float(hours[i]))
        country = str(visitor.attributes_on(day)["country"])
        session = simulate_human_session(rng, config, sampler, visitor, start, country)
        if day >= config.identity_start and visitor.user_id is not None:
            session = assign_identity(
                stream(config.seed, "identity", day, i),
                config,
                pool.device_cookies[visitor.visitor_idx],
                session,
            )
        events += apply_dirt(rng, config, session)
        if session.order is not None:
            intents.append(session.order)
    for i in range(n_bots):
        rng = stream(config.seed, "bot", day, i)
        start = midnight + timedelta(hours=float(bot_hours[i]))
        session = simulate_bot_session(rng, config, hot, start, str(rng.choice(COUNTRIES)))
        events += apply_dirt(rng, config, session)

    intents.sort(key=lambda o: o.created_at)
    orders = [
        Order(order_id_for(config, day, seq), o.created_at, o.cookie_id, o.user_id, o.items)
        for seq, o in enumerate(intents, start=1)
    ]
    return DaySimulation(day, events, orders)


def build_batch(
    config: GeneratorConfig, catalog: Catalog, pool: VisitorPool, arrival_day: date
) -> list[RawRow]:
    """All raw rows that reach the server on `arrival_day` (UTC), ordered by arrival time."""
    rows: list[RawRow] = []
    for back in range(lookback_days(config) + 1):
        sim = simulate_day(config, catalog, pool, arrival_day - timedelta(days=back))
        rows += [r for r in sim.events if r["server_ts"].date() == arrival_day]
    rows.sort(key=lambda r: (r["server_ts"], r["event_id"]))
    return rows


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    raise TypeError(f"not JSON serializable: {type(value)}")


def to_jsonl(rows: list[RawRow]) -> bytes:
    """One JSON object per line, UTF-8, timestamps as ISO-8601 UTC with milliseconds."""
    lines = (json.dumps(r, default=_json_default, separators=(",", ":")) for r in rows)
    return "".join(f"{line}\n" for line in lines).encode()
