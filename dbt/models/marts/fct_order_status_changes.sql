-- One row per status an order entered: its creation and every later transition, as a
-- transaction fact read straight from the change log (ADR 0007). Answers what fct_orders cannot
-- at its grain, such as how many orders shipped on a day, or how long orders wait in a status.
-- Snapshot rows are left out: they record a state, not a change. They still say since when the
-- order was in that state (its `updated_at`), which times the first transition after it.

with changes as (

    select * from {{ ref('stg_shop_cdc__order_changes') }}
    where change_type <> 'delete'

),

transitions as (

    select
        order_id,
        lsn,
        change_type,
        case when change_type = 'insert' then null else previous_status end as from_status,
        status as to_status,
        changed_at,
        lag(changed_at) over (partition by order_id order by log_position) as previous_changed_at
    from changes

)

select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'lsn']) }} as order_status_change_key,
    order_id,
    cast(strftime(changed_at at time zone 'UTC', '%Y%m%d') as integer) as date_key,
    from_status,
    to_status,
    changed_at,
    epoch(changed_at - previous_changed_at) / 3600 as hours_in_previous_status
from transitions
where change_type in ('insert', 'update')
