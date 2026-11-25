-- One row per change to an order in the shop's database, in commit order: its insert, each
-- status update, and for orders that existed before CDC started, the snapshot of their state then.
--
-- Debezium delivers at least once: after a restart it can send changes again that bronze already
-- has. A redelivery is the same change at the same position in the WAL (lsn), so the first
-- delivery is kept. Not deduplicated on the row image: an order can legitimately go back to an
-- earlier state.
--
-- `changed_at` is the shop's own (simulated) time of the change, from the row's `updated_at`.
-- `committed_at` is when Postgres committed it on the wall clock, which says when the generator
-- ran, not when the order changed.

with source as (

    select * from {{ source('shop_cdc', 'changes') }}
    -- Tests write to their own schemas of the same database (ADR 0007).
    where schema_name = 'public' and table_name = 'orders'

),

first_delivery as (

    select *
    from source
    qualify row_number() over (partition by key, op, lsn order by kafka_offset) = 1

),

parsed as (

    select
        cast(coalesce(after, before) ->> 'order_id' as bigint) as order_id,
        case op
            when 'c' then 'insert'
            when 'u' then 'update'
            when 'd' then 'delete'
            when 'r' then 'snapshot'
        end as change_type,
        after ->> 'status' as status,
        before ->> 'status' as previous_status,
        cast(after ->> 'user_id' as bigint) as user_id,
        after ->> 'cookie_id' as cookie_id,
        cast(after ->> 'total_cents' as bigint) as total_cents,
        cast(after ->> 'created_at' as timestamptz) as created_at,
        cast(coalesce(after, before) ->> 'updated_at' as timestamptz) as changed_at,
        source_ts as committed_at,
        lsn,
        kafka_offset as log_position,
        _ingested_at as ingested_at
    from first_delivery

)

select * from parsed
