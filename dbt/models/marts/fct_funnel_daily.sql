-- Conversion funnel per day, device and campaign. Only additive counts are stored; rates
-- (conversion, bounce) are ratios of sums and are computed where the data is read, so they stay
-- correct when the reader rolls days, devices or campaigns up.

with sessions as (

    select
        date_key,
        device,
        coalesce(campaign, '(none)') as utm_campaign,
        is_bounce,
        reached_product,
        reached_cart,
        reached_checkout
    from {{ ref('fct_sessions') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['date_key', 'device', 'utm_campaign']) }} as funnel_key,
    date_key,
    device,
    utm_campaign,
    count(*) as sessions,
    count(*) filter (where is_bounce) as bounced_sessions,
    count(*) filter (where reached_product) as product_sessions,
    count(*) filter (where reached_cart) as cart_sessions,
    count(*) filter (where reached_checkout) as checkout_sessions
from sessions
group by date_key, device, utm_campaign
