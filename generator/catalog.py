"""Product catalog with Zipf-distributed popularity."""

from dataclasses import dataclass

import numpy as np

from generator.config import GeneratorConfig
from generator.rng import stream

CATEGORY_NAMES = (
    "electronics",
    "home",
    "garden",
    "toys",
    "books",
    "sports",
    "fashion",
    "beauty",
    "grocery",
    "pets",
    "tools",
    "automotive",
)


@dataclass(frozen=True)
class Category:
    category_id: int
    slug: str


@dataclass(frozen=True)
class Product:
    product_id: int
    category_id: int
    price_cents: int


@dataclass(frozen=True)
class Catalog:
    categories: tuple[Category, ...]
    products: tuple[Product, ...]
    product_weights: np.ndarray  # Zipf popularity per product, sums to 1
    category_weights: np.ndarray  # total popularity per category, sums to 1

    def products_in(self, category_id: int) -> tuple[np.ndarray, np.ndarray]:
        """Indices into `products` for a category, and their popularity renormalised to 1."""
        idx = np.array(
            [i for i, p in enumerate(self.products) if p.category_id == category_id], dtype=int
        )
        w = self.product_weights[idx]
        return idx, w / w.sum()


def build_catalog(config: GeneratorConfig) -> Catalog:
    rng = stream(config.seed, "catalog")
    n, k = config.n_products, config.n_categories

    # Balanced but random: every category gets products, and category is independent of rank.
    category_of = rng.permutation(np.arange(n) % k) + 1

    # Zipf: the product of popularity rank r gets weight r**-s. Ranks are shuffled so that
    # popularity does not correlate with product_id.
    ranks = rng.permutation(n) + 1
    weights = ranks.astype(float) ** -config.zipf_exponent
    weights /= weights.sum()

    prices = np.round(rng.lognormal(mean=3.6, sigma=0.8, size=n) * 100).astype(int)
    prices = np.maximum(prices, 99)

    categories = tuple(
        Category(i + 1, CATEGORY_NAMES[i] if i < len(CATEGORY_NAMES) else f"category-{i + 1}")
        for i in range(k)
    )
    products = tuple(
        Product(product_id=i + 1, category_id=int(category_of[i]), price_cents=int(prices[i]))
        for i in range(n)
    )
    category_weights = np.bincount(category_of - 1, weights=weights, minlength=k)
    return Catalog(categories, products, weights, category_weights)
