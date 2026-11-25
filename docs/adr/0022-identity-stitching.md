# ADR 0022: Identity stitching, a cookie belongs to the one user who logs in on it

- **Status:** accepted
- **Date:** 2026-11-25
- **Versions:** dbt 2.0.12, DuckDB 2.0.0, Evidence 0.12.2.

## Context

The clickstream knows a cookie, and knows the user only on the pages viewed while logged in. A
cookie is a device, and one person has several: they browse on the phone, log in on the laptop
at the checkout. Counted by cookie, every new device is a new visitor who never comes back, and
the page views before a login belong to nobody. Retention reads too low, and features per user
(the feature store comes next) miss most of what the user did.

Until now the generator gave a visitor one cookie and logged a session in from its first page,
so there was nothing to stitch.

## Decision

- **The generator models devices and logins** (`generator/identity.py`), from
  `identity_start` (2026-03-01) on:
  - a registered user has a cookie per device type;
  - 3% of users share a desktop with another user (a family computer);
  - half of the logged-in sessions start logged in (a remembered device), the rest log in at a
    random page, at the checkout at the latest, and the page views before it have no `user_id`.

  Before that day the generator is unchanged. The choices come from their own random streams,
  so the days already loaded regenerate byte for byte, and so do the late events they still
  deliver.
- **The map is a model** (`int_identity__cookie_to_user`): one row per cookie that somebody
  logged in on. The evidence is deterministic only, a login: no fingerprinting by IP or user
  agent.
- **A cookie that exactly one user logged in on is that user's**, for all its page views. The
  mapping is retroactive: visits before the first login, even weeks earlier, are the user's.
- **A cookie that several users logged in on is a shared device.** Its logged-in sessions keep
  their user; its anonymous sessions are given to nobody.
- **The stitching is applied in `fct_sessions`, not in `fct_page_views`.** The page view fact is
  incremental and keeps the `user_id` as observed. A retroactive map changes old rows whenever
  a cookie gets its first login, and a merge with a lookback window cannot reach them. The
  session fact is a plain table rebuilt on every run, so it is retroactive for free.
- **The session fact gets three columns**, a non-breaking addition to version 2 (ADR 0025):
  - `identified_user_id`: the session's own login, or else the cookie's user;
  - `identity_source`: one of `login`, `stitched`, `shared_device` or `anonymous`;
  - `person_id`: `user:<id>`, or `cookie:<id>` when nobody is known.

  Version 1 is frozen and does not get them.
- **Retention counts people** (`fct_retention_weekly` on `person_id`), and the semantic layer
  has `people`, `identified_sessions` and `identified_session_share`.

## Alternatives considered

- **Stitching in `fct_page_views`.** Every page view would carry its stitched user. The
  incremental merge would need to recompute every row of a cookie that just logged in, so it
  would no longer be incremental, or it would leave old rows unstitched.
- **A shared cookie to the nearest login in time.** This is the usual heuristic: the anonymous
  session goes to whoever logged in last before it, or next after it. It is right in most
  households and wrong in a way that cannot be seen. One person's visits inflate another's
  activity, and the per-user features train on them. A missing link costs a visitor counted
  twice, which is the cost the warehouse already pays for anonymous visitors.
- **Not retroactive: only from the first login on.** This is simpler to reason about, because a
  past number never changes. But it loses the most useful part, the visits that led to the
  first login and the first order. A dashboard whose past moves after a late event is already
  the rule here (ADR 0004).
- **A graph of identities** (cookie–user–cookie edges, connected components). It is needed when
  there are several kinds of identifiers (email, device id, loyalty card). With one kind of
  login, the components are a user and their cookies, which the map already is. A component
  would also merge two users through a shared computer.
- **A dbt snapshot of the map** (SCD2 of who owned a cookie when). It records the history of a
  decision that is recomputed from complete data on every run, and it would make the result
  depend on the order in which the days were built.
- **Probabilistic matching** (IP, user agent, behaviour). There is nothing reliable to match on
  in the synthetic data, and a portfolio warehouse should not pretend there is.
- **Stitching from `identity_start` everywhere, or changing the generator for every day.**
  Changing every day would invalidate the days already loaded: a late event regenerated for
  February would come from another session than the one in bronze. The gate is the same device
  as `utm_start` (the schema change).

## Consequences

- A cookie's first login changes past sessions (`identity_source` `anonymous` → `stitched`) and
  moves the person to an earlier retention cohort. It works the other way too: when a second
  user first logs in on a cookie, its anonymous past becomes `shared_device`, and that cookie
  counts as one more person in the weeks before. The numbers before 2026-03-01 hardly change
  (on the replayed data, one person a week), because back then a cookie had one user.
- Checked against the generator's truth on 2026-03-01 to 03-03: no cookie was given to the
  wrong user. Nine cookies that the generator shares went to their only user who had logged in
  so far: two owners look like one until the second one logs in, which no rule can see.
- On the first replayed week, about 70% of human events are anonymous. Stitching gives a user to
  most sessions of cookies that are ever logged in on; a user with two devices is one person
  instead of two.
- `fct_visitor_conversion_daily` still counts by cookie: a product view on the phone and a
  checkout on the laptop are two visitors. Moving it to `person_id` changes its grain, which is a
  new version. It is left until a consumer needs it.
- The orders keep the session's cookie and the logged-in user. Joining an order to the session
  of its checkout goes through the cookie, as before.
- The CI days (2026-01-31 to 02-03) are before `identity_start`. The rules are covered by unit
  tests in Python (the generator) and dbt (the map, the session's identity, the retention), and
  were checked on the days from 2026-03-01 on.
