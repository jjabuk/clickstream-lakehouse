"""Who a session belongs to: a cookie per device, and a login that can come mid-session (ADR 0022).

From `config.identity_start` on, a registered user has one cookie per device type: the cookie of
the visitor pool on the device they use most, and a second one on the other. A few households
share a computer, so one desktop cookie belongs to two users. A logged-in session is logged in
from the first page when the device remembers the login; otherwise the login comes at a random
page, at the checkout at the latest, and the page views before it carry no `user_id`.

Before `identity_start` every visitor has one cookie and a logged-in session is logged in from
its first page, as the generator always did. The days already loaded stay reproducible: the
choices here come from their own random streams and change no draw of the session itself.
"""

from collections.abc import Mapping
from dataclasses import replace

import numpy as np

from generator.config import GeneratorConfig
from generator.sessions import Session


def assign_identity(
    rng: np.random.Generator,
    config: GeneratorConfig,
    cookies: Mapping[str, str],
    session: Session,
) -> Session:
    """The session on its device's cookie, and the page it logs in on."""
    cookie_id = cookies[session.device]
    logged_in_from = 0
    if session.user_id is not None and rng.random() >= config.remembered_login_prob:
        logged_in_from = int(rng.integers(len(session.events)))
        checkout = [i for i, e in enumerate(session.events) if e.page_type == "checkout"]
        if checkout:  # an order needs a login, so it comes at the checkout at the latest
            logged_in_from = min(logged_in_from, checkout[0])
    order = replace(session.order, cookie_id=cookie_id) if session.order else None
    return replace(session, cookie_id=cookie_id, logged_in_from=logged_in_from, order=order)
