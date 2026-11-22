-- One row per session with the ordered funnel steps it reached: a product view, then the cart,
-- then the checkout. A step counts only if it happened after the previous one in the same
-- session (a closed funnel), so a visitor who opens /cart straight from an e-mail link has not
-- reached the cart in the funnel sense.

with product_step as (

    select
        session_id,
        page_type,
        page_number_in_session,
        min(page_number_in_session) filter (where page_type = 'product')
            over (partition by session_id) as product_at
    from {{ ref('fct_page_views') }}

),

cart_step as (

    select
        session_id,
        page_type,
        page_number_in_session,
        product_at,
        min(page_number_in_session) filter (
            where page_type = 'cart' and page_number_in_session > product_at
        ) over (partition by session_id) as cart_at
    from product_step

),

steps as (

    select
        session_id,
        max(product_at) as product_at,
        max(cart_at) as cart_at,
        min(page_number_in_session) filter (
            where page_type = 'checkout' and page_number_in_session > cart_at
        ) as checkout_at
    from cart_step
    group by session_id

)

select
    session_id,
    product_at is not null as reached_product,
    cart_at is not null as reached_cart,
    checkout_at is not null as reached_checkout
from steps
