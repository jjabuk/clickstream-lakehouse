-- Identity stitching (ADR 0022): one row per cookie that somebody ever logged in on, and the user
-- it belongs to. A cookie is a device; a login is the only evidence of who uses it.
--
-- The rules:
-- - A cookie that one user logged in on is that user's device, from its first page view on,
--   before the first login too. Stitching is retroactive: the map is rebuilt on every run from all
--   logins, so a cookie's anonymous history moves to the user on the day they first log in.
-- - A cookie that two or more users logged in on is a shared device (a family computer). Its
--   logged-in page views keep their own user; its anonymous ones are not given to anybody.
--   Guessing by the nearest login would credit one person with another's visits, and a wrong
--   merge is worse for retention and per-user features than a missing one.
-- - Anonymous cookies, never logged in on, are not in the map and stay their own visitor.
--
-- Only humans count: bots send no cookie. add_to_cart events are logins too, so the source is
-- staging, not the page view fact.

select
    cookie_id,
    case when count(distinct user_id) = 1 then min(user_id) end as user_id,
    count(distinct user_id) > 1 as is_shared_device,
    count(distinct user_id) as users_logged_in,
    min(occurred_at) as first_login_at,
    max(occurred_at) as last_login_at
from {{ ref('stg_clickstream__events') }}
where not is_bot and user_id is not null
group by cookie_id
