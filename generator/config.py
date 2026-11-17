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
        ):
            if not 0.0 <= getattr(self, name) <= 1.0:
                raise ValueError(f"{name} must be within [0, 1]")
