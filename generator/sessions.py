"""Session simulation: the clean, true events of one visit, before any dirt is applied.

A human session is a walk on a Markov chain over page types (home -> category -> product ->
cart -> checkout), with an exit probability at every step. A bot session is a flat burst of
product page views on a few hot products, without cookie or user.
"""

import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np

from generator.catalog import Catalog
from generator.config import GeneratorConfig
from generator.visitors import Visitor

# Row = state, entries = (next state, probability). "purchase" completes an order and ends the
# session; "exit" ends it without one. The chain is a modelling choice, kept as data so it can
# be tuned without touching the code.
TRANSITIONS: dict[str, tuple[tuple[str, float], ...]] = {
    "start": (("home", 0.45), ("category", 0.25), ("product", 0.30)),
    "home": (("category", 0.55), ("product", 0.15), ("exit", 0.30)),
    "category": (("product", 0.60), ("category", 0.10), ("home", 0.05), ("exit", 0.25)),
    "product": (("product", 0.25), ("category", 0.15), ("cart", 0.10), ("exit", 0.50)),
    "cart": (("checkout", 0.45), ("product", 0.20), ("category", 0.05), ("exit", 0.30)),
    "checkout": (("purchase", 0.70), ("exit", 0.30)),
}
MAX_STEPS = 60  # hard cap on page views of one human session

USER_AGENTS = {
    "desktop": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 Safari/605",
        "Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
    ),
    "mobile": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 Mobile",
        "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/131.0 Mobile",
    ),
}
# Some bots announce themselves, some pretend to be a browser. Only the missing cookie and the
# request rate give the second kind away, which is what the bot filter in dbt has to learn.
BOT_USER_AGENTS = (
    "python-requests/2.32.3",
    "curl/8.7.1",
    "Googlebot/2.1 (+http://www.google.com/bot.html)",
    "AhrefsBot/7.0 (+http://ahrefs.com/robot/)",
    USER_AGENTS["desktop"][0],
    USER_AGENTS["desktop"][2],
)


@dataclass(frozen=True)
class TrueEvent:
    """What happened, at the true time, as the server would have seen it."""

    event_id: str
    event_type: str  # "page_view" or "add_to_cart"
    ts: datetime
    page_type: str  # home | category | product | cart | checkout
    page_url: str
    product_id: int | None = None
    quantity: int | None = None


@dataclass(frozen=True)
class OrderIntent:
    """An order placed at the end of a checkout. `order_id` is assigned per day later."""

    created_at: datetime
    cookie_id: str
    user_id: int | None
    items: tuple[tuple[int, int, int], ...]  # (product_id, quantity, unit_price_cents)


@dataclass(frozen=True)
class Session:
    cookie_id: str | None
    user_id: int | None
    country: str
    device: str
    user_agent: str
    events: tuple[TrueEvent, ...]
    order: OrderIntent | None = None
    # Index of the first event that carries user_id: a login mid-session (generator/identity.py).
    logged_in_from: int = 0


def _round_ms(ts: datetime) -> datetime:
    return ts.replace(microsecond=ts.microsecond // 1000 * 1000)


def _event_ids(rng: np.random.Generator, n: int) -> list[str]:
    raw = rng.bytes(16 * n)
    return [str(uuid.UUID(bytes=raw[16 * i : 16 * i + 16], version=4)) for i in range(n)]


class CatalogSampler:
    """Catalog lookups used while walking the chain, precomputed once per day."""

    def __init__(self, catalog: Catalog):
        self.catalog = catalog
        self.by_category = {
            c.category_id: catalog.products_in(c.category_id) for c in catalog.categories
        }
        self.category_ids = np.array([c.category_id for c in catalog.categories])

    def category(self, rng: np.random.Generator) -> int:
        return int(rng.choice(self.category_ids, p=self.catalog.category_weights))

    def product(self, rng: np.random.Generator, category_id: int | None) -> int:
        """A product, from `category_id` if given, otherwise from the whole catalog (Zipf)."""
        if category_id is None:
            idx = int(rng.choice(len(self.catalog.products), p=self.catalog.product_weights))
        else:
            ids, w = self.by_category[category_id]
            idx = int(rng.choice(ids, p=w))
        return self.catalog.products[idx].product_id


def _next_state(rng: np.random.Generator, state: str) -> str:
    u, acc = rng.random(), 0.0
    for nxt, p in TRANSITIONS[state]:
        acc += p
        if u < acc:
            return nxt
    return TRANSITIONS[state][-1][0]


def simulate_human_session(
    rng: np.random.Generator,
    config: GeneratorConfig,
    sampler: CatalogSampler,
    visitor: Visitor,
    start: datetime,
    country: str,
) -> Session:
    products = sampler.catalog.products
    device = "mobile" if rng.random() < visitor.mobile_share else "desktop"
    user_agent = str(rng.choice(USER_AGENTS[device]))
    user_id = visitor.user_id if rng.random() < config.login_prob else None

    def gap() -> float:
        if rng.random() < config.long_pause_prob:
            return float(rng.uniform(31 * 60, 120 * 60))  # longer than the session timeout
        return float(np.clip(rng.lognormal(math.log(15), 0.8), 0.5, 25 * 60))

    raw: list[tuple[str, datetime, str, str, int | None, int | None]] = []
    cart: dict[int, int] = {}
    order: OrderIntent | None = None
    ts, state = start, "start"
    category_id: int | None = None
    product_id: int | None = None
    page_views = 0

    def emit(event_type: str, page_type: str, url: str, pid: int | None, qty: int | None) -> None:
        raw.append((event_type, ts, page_type, url, pid, qty))

    while page_views < MAX_STEPS:
        state = _next_state(rng, state)
        if state == "exit":
            break
        if state == "purchase":
            ts += timedelta(seconds=float(rng.uniform(20, 120)))
            order = OrderIntent(
                created_at=_round_ms(ts),
                cookie_id=visitor.cookie_id,
                user_id=user_id,
                items=tuple(
                    (pid, qty, products[pid - 1].price_cents) for pid, qty in sorted(cart.items())
                ),
            )
            break
        if page_views:
            ts += timedelta(seconds=gap())
        page_views += 1
        if state == "home":
            emit("page_view", "home", "/", None, None)
        elif state == "category":
            category_id = sampler.category(rng)
            emit("page_view", "category", f"/c/{category_id}", None, None)
        elif state == "product":
            stay = category_id is not None and rng.random() < 0.6
            product_id = sampler.product(rng, category_id if stay else None)
            category_id = products[product_id - 1].category_id
            emit("page_view", "product", f"/p/{product_id}", product_id, None)
        elif state == "cart":
            assert product_id is not None  # the chain reaches the cart only from a product page
            qty = 1 + int(rng.random() < 0.2)
            cart[product_id] = cart.get(product_id, 0) + qty
            ts += timedelta(seconds=float(rng.uniform(1, 6)))
            emit("add_to_cart", "product", f"/p/{product_id}", product_id, qty)
            ts += timedelta(seconds=float(rng.uniform(1, 6)))
            emit("page_view", "cart", "/cart", None, None)
        elif state == "checkout":
            emit("page_view", "checkout", "/checkout", None, None)

    ids = _event_ids(rng, len(raw))
    events = tuple(
        TrueEvent(ids[i], et, _round_ms(t), pt, url, pid, qty)
        for i, (et, t, pt, url, pid, qty) in enumerate(raw)
    )
    return Session(visitor.cookie_id, user_id, country, device, user_agent, events, order)


def simulate_bot_session(
    rng: np.random.Generator,
    config: GeneratorConfig,
    hot_products: np.ndarray,
    start: datetime,
    country: str,
) -> Session:
    """A crawler: hundreds of requests a few hundred ms apart, concentrated on hot products."""
    n = max(1, int(rng.poisson(config.bot_events_per_session)))
    gaps = rng.exponential(0.4, size=n).clip(0.05, 5.0)
    offsets = np.cumsum(gaps)
    # Zipf-like concentration: the first hot product gets the most traffic.
    weights = 1.0 / np.arange(1, len(hot_products) + 1)
    pids = rng.choice(hot_products, size=n, p=weights / weights.sum())
    ids = _event_ids(rng, n)
    events = tuple(
        TrueEvent(
            ids[i],
            "page_view",
            _round_ms(start + timedelta(seconds=float(offsets[i]))),
            "product",
            f"/p/{int(pids[i])}",
            int(pids[i]),
        )
        for i in range(n)
    )
    user_agent = str(rng.choice(BOT_USER_AGENTS))
    return Session(None, None, country, "desktop", user_agent, events)
