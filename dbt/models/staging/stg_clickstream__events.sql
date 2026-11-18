-- One row per event. Bronze keeps every delivery; duplicates are removed here, keeping the first
-- delivery of each event_id.
with source as (

    select * from {{ source('bronze', 'clickstream_events') }}

),

first_delivery as (

    select *
    from source
    qualify row_number() over (partition by event_id order by server_ts, _ingested_at) = 1

),

renamed as (

    select
        event_id,
        event_type,

        event_ts as client_ts,
        server_ts as received_at,
        -- An event cannot happen after it arrived, which repairs a client clock that runs ahead.
        -- A clock that runs behind looks like lateness and cannot be told apart here; it shifts
        -- the whole session by a constant, so the gaps inside the session stay correct.
        least(event_ts, server_ts) as occurred_at,

        cookie_id,
        user_id,
        country,
        device,
        user_agent,
        page_type,
        page_url,
        product_id,
        quantity,
        utm_campaign,

        _ingested_at as ingested_at
    from first_delivery

)

select
    *,
    epoch(received_at) - epoch(occurred_at) as arrival_lag_seconds,
    -- Bots send no cookie. Some also announce themselves in the user agent.
    (
        cookie_id is null
        or regexp_matches(lower(user_agent), 'bot|crawler|spider|curl|python-requests')
    ) as is_bot
from renamed
