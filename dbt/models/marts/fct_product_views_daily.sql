-- Interest in each product per day: views of its product page by humans and adds to cart.
-- A sparse fact: a product has a row only on days with at least one view or add to cart.
--
-- Page views come from fct_page_views, which already drops bots and duplicates. Adds to cart are
-- not page views, so they come from staging with the same bot filter.

with product_events as (

    select
        occurred_at,
        product_id,
        'page_view' as event_type,
        session_id,
        cookie_id
    from {{ ref('fct_page_views') }}
    where page_type = 'product'

    union all

    select
        occurred_at,
        product_id,
        'add_to_cart' as event_type,
        cast(null as varchar) as session_id,
        cookie_id
    from {{ ref('stg_clickstream__events') }}
    where event_type = 'add_to_cart' and not is_bot

),

daily as (

    select
        cast(strftime(occurred_at at time zone 'UTC', '%Y%m%d') as integer) as date_key,
        cast(occurred_at at time zone 'UTC' as date) as date_day,
        product_id,
        count(*) filter (where event_type = 'page_view') as page_views,
        count(distinct session_id) filter (where event_type = 'page_view') as sessions,
        count(distinct cookie_id) filter (where event_type = 'page_view') as visitors,
        count(*) filter (where event_type = 'add_to_cart') as add_to_carts
    from product_events
    group by 1, 2, 3

)

select
    {{ dbt_utils.generate_surrogate_key(['daily.date_key', 'daily.product_id']) }}
        as product_day_key,
    daily.date_key,
    daily.date_day,
    daily.product_id,
    products.category_id,
    daily.page_views,
    daily.sessions,
    daily.visitors,
    daily.add_to_carts
from daily
left join {{ ref('stg_shop__products') }} as products
    on daily.product_id = products.product_id
