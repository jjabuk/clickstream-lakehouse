-- Raw traffic per day, split into humans and bots, to show what bot filtering removes. Counts
-- events after deduplication; bot events never reach fct_page_views or any other mart.

select
    {{ dbt_utils.generate_surrogate_key(['date_key', 'is_bot']) }} as traffic_key,
    date_key,
    is_bot,
    count(*) as events,
    count(*) filter (where event_type = 'page_view') as page_view_events,
    count(*) filter (where event_type = 'add_to_cart') as add_to_cart_events
from (
    select
        cast(strftime(occurred_at at time zone 'UTC', '%Y%m%d') as integer) as date_key,
        is_bot,
        event_type
    from {{ ref('stg_clickstream__events') }}
) as events
group by date_key, is_bot
