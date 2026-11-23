-- One row per visitor (cookie) and day with a product view: did the visitor reach a checkout
-- page within 7 days after their first product view of that day, in any session?
--
-- Session funnel rates miss a visitor who views a product today and buys three days later. This
-- fact keeps that conversion as additive flags, so the rate is a ratio of sums at any grain
-- (ADR 0023). MetricFlow's own conversion metric was tried and is not used: its inputs must be
-- unfiltered counts, and without a filter every page view becomes a base event.
--
-- Right censoring: a day whose 7-day window has not closed yet can still convert. Its row says
-- so (is_window_complete), and readers should leave it out of rates; otherwise the latest days
-- always look like a drop in conversion.

with product_days as (

    select
        cookie_id,
        cast(occurred_at at time zone 'UTC' as date) as product_day,
        min(occurred_at) as product_viewed_at,
        arg_min(device, occurred_at) as device
    from {{ ref('fct_page_views') }}
    where page_type = 'product'
    group by cookie_id, product_day

),

checkouts as (

    select
        cookie_id,
        occurred_at as checkout_at
    from {{ ref('fct_page_views') }}
    where page_type = 'checkout'

),

data_end as (

    select max(occurred_at) as last_event_at from {{ ref('fct_page_views') }}

),

converted as (

    select
        product_days.cookie_id,
        product_days.product_day,
        min(checkouts.checkout_at) as converted_at
    from product_days
    inner join checkouts
        on
            product_days.cookie_id = checkouts.cookie_id
            and product_days.product_viewed_at < checkouts.checkout_at
            and product_days.product_viewed_at + interval 7 day >= checkouts.checkout_at
    group by product_days.cookie_id, product_days.product_day

)

select
    {{ dbt_utils.generate_surrogate_key(['product_days.cookie_id', 'product_days.product_day']) }}
        as visitor_day_key,
    product_days.cookie_id,
    cast(strftime(product_days.product_day, '%Y%m%d') as integer) as date_key,
    product_days.product_day,
    product_days.product_viewed_at,
    product_days.device,
    converted.converted_at,
    converted.converted_at is not null as is_converted_7d,
    product_days.product_viewed_at + interval 7 day <= data_end.last_event_at
        as is_window_complete
from product_days
cross join data_end
left join converted
    on
        product_days.cookie_id = converted.cookie_id
        and product_days.product_day = converted.product_day
