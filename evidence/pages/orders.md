# Orders

The shop's orders do not come from a nightly copy of its database but from its change log:
Debezium reads every insert and status update from Postgres's WAL (`fct_orders`, ADR 0007). So
each order keeps the time of every milestone, which the shop's own table, holding only the
current status, has already overwritten.

{% row %}
{% big_value metric="orders" /%}
{% big_value metric="net_revenue" /%}
{% big_value metric="average_order_value" /%}
{% big_value metric="return_rate" /%}
{% big_value metric="cancellation_rate" /%}
{% /row %}

{% bar_chart
  metric="orders"
  series="status"
  stacked=true
  handle_missing="gaps"
  title="Orders per day, by current status"
/%}

Recent orders are still on their way: they are paid or shipped today and delivered, or returned,
in the coming days. Return rate counts only delivered orders for that reason.

## Lifecycle

```sql transitions
select coalesce(from_status, '(new)') || ' → ' || to_status as transition,
       count(*) as changes,
       round(avg(hours_in_previous_status)::numeric, 1) as avg_hours_before
from marts.fct_order_status_changes
group by 1
order by changes desc
```

{% table data="transitions" title="Status changes captured, and the hours spent in the status before" /%}

```sql history
select case when is_history_complete then 'from its insert' else 'from the CDC snapshot' end as history,
       count(*) as orders,
       count(paid_at) as with_paid_at,
       count(delivered_at) as with_delivered_at
from marts.fct_orders
group by 1
```

{% table data="history" title="Where each order's history starts" /%}

History starts when CDC started. An order placed before that entered the log through
Debezium's initial snapshot, with only its state at that moment, so its earlier milestones are
unknown. Its status, total and creation time are complete, and so are the counts and revenue
above; durations use only the milestones that were captured.
