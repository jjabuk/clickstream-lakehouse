-- One row per session, aggregated from fct_page_views.
--
-- A plain table on purpose. A late page view can bridge two sessions into one, and then the
-- later session's id disappears. An incremental merge on session_id cannot delete the vanished
-- row and would leave a phantom session; rebuilding from the (incremental) fact is correct by
-- construction and cheap, since it is one aggregation.

select
    session_id,
    any_value(cookie_id) as cookie_id,
    any_value(user_id) filter (where user_id is not null) as user_id,
    any_value(user_key) as user_key,
    min(occurred_at) as session_started_at,
    max(occurred_at) as session_ended_at,
    cast(strftime(min(occurred_at) at time zone 'UTC', '%Y%m%d') as integer) as date_key,
    epoch(max(occurred_at)) - epoch(min(occurred_at)) as duration_seconds,
    count(*) as page_views,
    count(*) = 1 as is_bounce,
    arg_min(page_type, occurred_at) as entry_page_type,
    arg_max(page_type, occurred_at) as exit_page_type,
    count(*) filter (where page_type = 'product') as product_views,
    bool_or(page_type = 'cart') as reached_cart,
    bool_or(page_type = 'checkout') as reached_checkout,
    any_value(device) as device,
    any_value(country) as country,
    any_value(utm_campaign) filter (where utm_campaign is not null) as utm_campaign,
    max(received_at) as last_received_at
from {{ ref('fct_page_views') }}
group by session_id
