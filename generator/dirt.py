"""Turn the true events of a session into the raw rows the ingestion actually receives.

Four kinds of dirt, all driven by the session's own random stream:

- client clock skew: `event_ts` comes from the client clock, `server_ts` from the server;
- late arrival: a share of events reaches the server 1 h to `late_max_hours` after it happened;
- duplicates: a share of events is delivered twice with the same `event_id`;
- schema drift: `utm_campaign` exists in the payload only from `config.utm_start` on.

`server_ts` is the arrival time. It decides which daily batch a row lands in, so a late event
shows up in the batch of the day it arrives, not the day it happened.
"""

from datetime import datetime, timedelta
from typing import Any

import numpy as np

from generator.config import GeneratorConfig
from generator.sessions import Session

RawRow = dict[str, Any]


def _round_ms(ts: datetime) -> datetime:
    return ts.replace(microsecond=ts.microsecond // 1000 * 1000)


def apply_dirt(rng: np.random.Generator, config: GeneratorConfig, session: Session) -> list[RawRow]:
    n = len(session.events)
    if n == 0:
        return []

    # One clock offset per session: most clients are within a couple of seconds, a few are far off.
    if rng.random() < config.skewed_session_rate:
        skew = float(rng.choice([-1, 1]) * rng.uniform(60, 2 * 3600))
    else:
        skew = float(rng.normal(0.0, 1.5))

    latency = rng.lognormal(np.log(0.3), 0.5, size=n)  # seconds from the event to the server
    is_late = rng.random(n) < config.late_rate
    late_delay = rng.uniform(3600, config.late_max_hours * 3600, size=n)
    delay = np.where(is_late, late_delay, latency)

    is_dup = rng.random(n) < config.duplicate_rate
    quick_retry = rng.random(n) < 0.8  # double-send within seconds; otherwise a retry hours later
    dup_delay = np.where(
        quick_retry, rng.uniform(0.1, 5.0, size=n), rng.uniform(3600, 6 * 3600, size=n)
    )

    carries_utm = rng.random() < config.utm_share
    campaign = str(rng.choice(config.utm_campaigns)) if carries_utm else None

    rows: list[RawRow] = []
    for i, ev in enumerate(session.events):
        server_ts = _round_ms(ev.ts + timedelta(seconds=float(delay[i])))
        row: RawRow = {
            "event_id": ev.event_id,
            "event_type": ev.event_type,
            "event_ts": _round_ms(ev.ts + timedelta(seconds=skew)),
            "server_ts": server_ts,
            "cookie_id": session.cookie_id,
            "user_id": session.user_id,
            "country": session.country,
            "device": session.device,
            "user_agent": session.user_agent,
            "page_type": ev.page_type,
            "page_url": ev.page_url,
            "product_id": ev.product_id,
            "quantity": ev.quantity,
        }
        if ev.ts.date() >= config.utm_start:
            row["utm_campaign"] = campaign
        rows.append(row)
        if is_dup[i]:
            rows.append(
                {**row, "server_ts": _round_ms(server_ts + timedelta(seconds=float(dup_delay[i])))}
            )
    return rows
