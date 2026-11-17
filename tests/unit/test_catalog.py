import numpy as np
import pytest

from generator.catalog import build_catalog
from generator.config import GeneratorConfig

CONFIG = GeneratorConfig(seed=7)


def test_catalog_is_deterministic():
    a, b = build_catalog(CONFIG), build_catalog(CONFIG)
    assert a.products == b.products
    assert np.array_equal(a.product_weights, b.product_weights)
    assert build_catalog(GeneratorConfig(seed=8)).products != a.products


def test_sizes_and_ids():
    catalog = build_catalog(CONFIG)
    assert len(catalog.categories) == CONFIG.n_categories
    assert [p.product_id for p in catalog.products] == list(range(1, CONFIG.n_products + 1))
    assert all(p.price_cents >= 99 for p in catalog.products)


def test_every_category_has_products():
    catalog = build_catalog(CONFIG)
    used = {p.category_id for p in catalog.products}
    assert used == {c.category_id for c in catalog.categories}


def test_weights_are_probabilities():
    catalog = build_catalog(CONFIG)
    assert catalog.product_weights.sum() == pytest.approx(1.0)
    assert catalog.category_weights.sum() == pytest.approx(1.0)
    for category in catalog.categories:
        _, w = catalog.products_in(category.category_id)
        assert w.sum() == pytest.approx(1.0)


def test_popularity_follows_zipf():
    """The top 10% of products carry most of the traffic, as a Zipf law with s=1.1 predicts."""
    catalog = build_catalog(CONFIG)
    ordered = np.sort(catalog.product_weights)[::-1]
    top_decile = ordered[: CONFIG.n_products // 10].sum()
    assert top_decile > 0.6
    # Weight of rank r relative to rank 1 is r**-s.
    assert ordered[9] / ordered[0] == pytest.approx(10**-CONFIG.zipf_exponent)


def test_popularity_does_not_depend_on_product_id():
    catalog = build_catalog(CONFIG)
    first_decile_by_id = catalog.product_weights[: CONFIG.n_products // 10].sum()
    assert first_decile_by_id < 0.6
