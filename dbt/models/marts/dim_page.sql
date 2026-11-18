-- One row per page URL of the shop: the three fixed pages, every category and every product.
-- page_key is a hash of page_url, the same expression the fact uses.

with fixed_pages as (

    select '/' as page_url, 'home' as page_type, null as category_id, null as product_id
    union all
    select '/cart', 'cart', null, null
    union all
    select '/checkout', 'checkout', null, null

),

category_pages as (

    select '/c/' || category_id as page_url, 'category' as page_type, category_id, null as product_id
    from {{ ref('stg_shop__categories') }}

),

product_pages as (

    select
        '/p/' || products.product_id as page_url,
        'product' as page_type,
        products.category_id,
        products.product_id
    from {{ ref('stg_shop__products') }} as products

),

pages as (

    select * from fixed_pages
    union all
    select * from category_pages
    union all
    select * from product_pages

)

select
    {{ dbt_utils.generate_surrogate_key(['pages.page_url']) }} as page_key,
    pages.page_url,
    pages.page_type,
    pages.category_id,
    categories.category_slug,
    pages.product_id
from pages
left join {{ ref('stg_shop__categories') }} as categories on pages.category_id = categories.category_id
