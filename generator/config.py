"""Generator configuration. Every random choice derives from `seed`, so runs are reproducible."""

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 42

    # First day of the simulated world. Visitor attribute changes are scheduled from here.
    epoch: date = date(2026, 1, 1)

    # Catalog
    n_categories: int = 12
    n_products: int = 300
    zipf_exponent: float = 1.1  # popularity of the product of rank r is proportional to r**-s

    # Visitors
    n_visitors: int = 3000
    registered_share: float = 0.4  # share of visitors that log in (have a user_id)
    premium_share: float = 0.2  # share of registered users starting on the premium plan
    country_change_prob: float = 0.10  # registered user moves country once within the horizon
    plan_change_prob_free: float = 0.25  # free user upgrades once within the horizon
    plan_change_prob_premium: float = 0.15  # premium user downgrades once within the horizon
    attribute_horizon_days: int = 365  # attribute changes happen within epoch + horizon

    # Human traffic: sessions start as an inhomogeneous Poisson process,
    # lambda(t) = lambda0 * (1 + a * sin(2*pi*(t - peak + 6h) / 24h)); the peak is `peak_hour_utc`.
    sessions_per_day: int = 1500  # expected number of human sessions per day (= 24 * lambda0)
    diurnal_amplitude: float = 0.6  # `a` above; must stay below 1 so lambda(t) is positive
    peak_hour_utc: float = 19.0
    login_prob: float = 0.85  # a registered visitor is logged in during a given session
    long_pause_prob: float = 0.03  # a gap between two events exceeds the 30-minute session timeout

    # Identity (ADR 0022): from this day on, a registered user has a cookie per device type, a few
    # share a computer, and a login can come in the middle of a session.
    identity_start: date = date(2026, 3, 1)
    remembered_login_prob: float = 0.5  # a logged-in session is logged in from its first page
    shared_device_share: float = 0.03  # registered users whose desktop is another user's

    # Bots: no cookie, no user_id, high request rate, a few hot products.
    bot_sessions_per_day: int = 30
    bot_events_per_session: int = 150
    n_hot_products: int = 10

    # Dirt applied to the raw feed
    duplicate_rate: float = 0.01  # share of events delivered twice with the same event_id
    late_rate: float = 0.03  # share of events that arrive late
    late_max_hours: int = 48  # a late event arrives 1 h .. late_max_hours after it happened
    skewed_session_rate: float = 0.02  # sessions whose client clock is minutes to hours off

    # Schema evolution: `utm_campaign` appears in the raw events from this day on.
    utm_start: date = date(2026, 2, 1)
    utm_share: float = 0.3  # share of sessions that carry a campaign
    utm_campaigns: tuple[str, ...] = ("spring_sale", "newsletter", "retargeting", "partner_x")

    def __post_init__(self) -> None:
        if self.seed < 0:
            raise ValueError("seed must be non-negative")
        if self.n_products < self.n_categories:
            raise ValueError("n_products must be at least n_categories")
        if self.n_visitors < 1 or self.attribute_horizon_days < 1:
            raise ValueError("n_visitors and attribute_horizon_days must be positive")
        for name in (
            "registered_share",
            "premium_share",
            "country_change_prob",
            "plan_change_prob_free",
            "plan_change_prob_premium",
            "login_prob",
            "long_pause_prob",
            "remembered_login_prob",
            "shared_device_share",
            "duplicate_rate",
            "late_rate",
            "skewed_session_rate",
            "utm_share",
        ):
            if not 0.0 <= getattr(self, name) <= 1.0:
                raise ValueError(f"{name} must be within [0, 1]")
        if not 0.0 <= self.diurnal_amplitude < 1.0:
            raise ValueError("diurnal_amplitude must be within [0, 1)")
        if self.sessions_per_day < 0 or self.bot_sessions_per_day < 0:
            raise ValueError("session counts must not be negative")
        if self.bot_events_per_session < 1 or not 1 <= self.n_hot_products <= self.n_products:
            raise ValueError("invalid bot settings")
        if self.late_max_hours < 2:
            raise ValueError("late_max_hours must be at least 2")
