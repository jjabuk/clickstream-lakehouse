-- One row per event. Bronze keeps every delivery; duplicates are removed here, keeping the first
-- delivery of each event_id.
--
-- Bronze evolves its schema: utm_campaign is added on the first day a batch carries it. Before
-- that day the column does not exist, so staging supplies it as null and the models downstream
-- keep one shape whichever day the warehouse is built from.
{%- set source_columns = adapter.get_columns_in_relation(source('clickstream', 'events'))
    | map(attribute='name') | map('lower') | list %}

with source as (

    select * from {{ source('clickstream', 'events') }}

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
        {% if 'utm_campaign' in source_columns -%}
            utm_campaign,
        {%- else -%}
            cast(null as varchar) as utm_campaign,
        {%- endif %}

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
