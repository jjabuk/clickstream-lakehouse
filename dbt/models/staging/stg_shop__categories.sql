select
    category_id,
    slug as category_slug
from {{ source('shop', 'categories') }}
