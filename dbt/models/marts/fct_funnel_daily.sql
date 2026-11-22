-- Conversion funnel per day, device and campaign. Only additive counts are stored; rates
-- (conversion, bounce) are ratios of sums and are computed where the data is read, so they stay
-- correct when the reader rolls days, devices or campaigns up.

with sessions as (

    select
        fct_sessions.date_key,
        fct_sessions.device,
        coalesce(fct_sessions.utm_campaign, '(none)') as utm_campaign,
        fct_sessions.is_bounce,
        steps.reached_product,
        steps.reached_cart,
        steps.reached_checkout
    from {{ ref('fct_sessions') }} as fct_sessions
    inner join {{ ref('int_sessions__funnel_steps') }} as steps
        on fct_sessions.session_id = steps.session_id

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
