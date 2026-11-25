-- One row per order line, as it is now: the latest image of each line in the change log. The shop
-- never changes a line after the order is placed, but the model does not rely on it; a deleted
-- line is left out.

with source as (

    select * from {{ source('shop_cdc', 'changes') }}
    where schema_name = 'public' and table_name = 'order_items'

),

latest as (

    select *
    from source
    qualify row_number() over (partition by key order by kafka_offset desc) = 1

)

select
    cast(after ->> 'order_id' as bigint) as order_id,
    cast(after ->> 'line_no' as integer) as line_no,
    cast(after ->> 'product_id' as integer) as product_id,
    cast(after ->> 'quantity' as integer) as quantity,
    cast(after ->> 'unit_price_cents' as bigint) as unit_price_cents,
    cast(after ->> 'quantity' as integer) * cast(after ->> 'unit_price_cents' as bigint)
        as line_total_cents
from latest
where op <> 'd'
