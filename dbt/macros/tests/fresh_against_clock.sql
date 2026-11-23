{#- Fails when the newest row is more than `max_hours` older than the simulated clock (or there
    are no rows): data stopped arriving although the clock moved on. dbt's source freshness and
    dbt_utils.recency compare with the wall clock, which simulated time never matches. -#}
{% test fresh_against_clock(model, timestamp_column, max_hours) %}

select
    max({{ timestamp_column }}) as newest_row_at,
    {{ simulated_now() }} as now_at
from {{ model }}
having
    max({{ timestamp_column }}) is null
    or max({{ timestamp_column }}) < {{ simulated_now() }} - interval '{{ max_hours }} hours'

{% endtest %}
