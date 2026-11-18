select
    user_id,
    country,
    plan,
    created_at,
    updated_at
from {{ source('shop', 'users') }}
