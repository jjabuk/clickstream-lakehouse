-- One row per order: an accumulating snapshot fact (Kimball). The row is updated as the order
-- moves through its lifecycle, and keeps a timestamp per milestone, which the source row in
-- Postgres cannot: it holds only the current status. The milestones come from the change log
-- (ADR 0007).
--
-- History starts when CDC started. An order that already existed then entered the log through
-- Debezium's snapshot, with only its state at that moment: its earlier milestones are unknown,
-- and `is_history_complete` is false. Its current status, total and creation time are still
-- right, so counts and revenue use those; only durations need complete histories.
--
-- Rejected: rebuilding the milestones from the generator's deterministic timeline. A warehouse
-- cannot know what the source does not record; that is the point of capturing changes.

with changes as (

    select * from {{ ref('stg_shop_cdc__order_changes') }}

),

latest as (

    select *
    from changes
    where change_type <> 'delete'
    qualify row_number() over (partition by order_id order by log_position desc) = 1

),

milestones as (

    select
        order_id,
        min(changed_at) filter (where status = 'paid') as paid_at,
        min(changed_at) filter (where status = 'shipped') as shipped_at,
        min(changed_at) filter (where status = 'delivered') as delivered_at,
        min(changed_at) filter (where status = 'returned') as returned_at,
        min(changed_at) filter (where status = 'cancelled') as cancelled_at,
        bool_or(change_type = 'insert') as is_history_complete,
        count(*) filter (where change_type = 'update') as status_changes
    from changes
    group by order_id

),

items as (

    select
        order_id,
        count(*) as line_count,
        cast(sum(quantity) as bigint) as units,
        sum(line_total_cents) as items_total_cents
    from {{ ref('stg_shop_cdc__order_items') }}
    group by order_id

),

orders as (

    select
        latest.order_id,
        latest.user_id,
        latest.cookie_id,
        latest.created_at,
        latest.status,
        latest.changed_at as status_changed_at,
        milestones.paid_at,
        milestones.shipped_at,
        milestones.delivered_at,
        milestones.returned_at,
        milestones.cancelled_at,
        milestones.is_history_complete,
        milestones.status_changes,
        latest.total_cents,
        coalesce(items.line_count, 0) as line_count,
        coalesce(items.units, 0) as units,
        items.items_total_cents
    from latest
    inner join milestones on latest.order_id = milestones.order_id
    left join items on latest.order_id = items.order_id

)

select
    orders.order_id,
    coalesce(dim_user.user_key, md5('unknown user')) as user_key,
    orders.user_id,
    orders.cookie_id,
    cast(strftime(orders.created_at at time zone 'UTC', '%Y%m%d') as integer) as date_key,
    orders.created_at,
    orders.status,
    orders.status_changed_at,
    orders.paid_at,
    orders.shipped_at,
    orders.delivered_at,
    orders.returned_at,
    orders.cancelled_at,
    -- From the current status, so they hold for orders whose history began with the snapshot.
    orders.status in ('paid', 'shipped', 'delivered', 'returned') as is_paid,
    orders.status in ('delivered', 'returned') as is_delivered,
    orders.status = 'returned' as is_returned,
    orders.status = 'cancelled' as is_cancelled,
    orders.status in ('delivered', 'returned', 'cancelled') as is_closed,
    orders.is_history_complete,
    orders.status_changes,
    orders.line_count,
    orders.units,
    cast(orders.total_cents / 100 as decimal(18, 2)) as order_total,
    cast(
        case when orders.status in ('paid', 'shipped', 'delivered') then orders.total_cents else 0 end
        / 100 as decimal(18, 2)
    ) as net_revenue,
    orders.items_total_cents = orders.total_cents as is_total_consistent,
    epoch(orders.shipped_at - orders.paid_at) / 3600 as hours_paid_to_shipped,
    epoch(orders.delivered_at - orders.created_at) / 3600 as hours_created_to_delivered
from orders
left join {{ ref('dim_user') }} as dim_user
    on
        orders.user_id = dim_user.user_id
        and orders.created_at >= dim_user.effective_from
        and orders.created_at < dim_user.effective_to
