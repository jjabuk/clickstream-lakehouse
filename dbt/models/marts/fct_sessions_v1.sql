-- Version 1 of fct_sessions, kept for its consumers until its deprecation date (ADR 0025).
-- The rows of version 2; only the campaign column keeps its old name, `utm_campaign`.

select
    session_id,
    cookie_id,
    user_id,
    user_key,
    session_started_at,
    session_ended_at,
    date_key,
    duration_seconds,
    page_views,
    is_bounce,
    entry_page_type,
    exit_page_type,
    product_views,
    device,
    country,
    last_received_at,
    reached_product,
    reached_cart,
    reached_checkout,
    campaign as utm_campaign
from {{ ref('fct_sessions', v=2) }}
