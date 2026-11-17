"""Named, independent random streams derived from one seed.

Each stream is addressed by keys, e.g. `stream(seed, "traffic", day)`. Streams do not share
state, so the output for one day never depends on whether other days were generated before it.
That is what lets a batch for any day be regenerated on its own.
"""

import zlib
from datetime import date

import numpy as np

Key = str | int | date


def _key_to_int(key: Key) -> int:
    if isinstance(key, date):
        return key.toordinal()
    if isinstance(key, str):
        return zlib.crc32(key.encode())
    if key < 0:
        raise ValueError("integer keys must be non-negative")
    return key


def stream(seed: int, *keys: Key) -> np.random.Generator:
    """Return a generator that is a pure function of `seed` and `keys`."""
    return np.random.default_rng(np.random.SeedSequence([seed, *map(_key_to_int, keys)]))
