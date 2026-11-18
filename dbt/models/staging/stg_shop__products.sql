select
    product_id,
    category_id,
    price_cents
from {{ source('shop', 'products') }}
