{#- The row count of the last complete day before the simulated clock, scored against the days
    before it: fails when it is more than `sensitivity` standard deviations from their mean.
    A lost file, a duplicated load or a broken filter all show up as a jump in volume.

    Days without rows are left out of the training window (the dev data has a gap), and the test
    stays silent until there are `min_training_days` days to learn from. The tested day itself
    counts as 0 if it has no rows. A z-score over the trailing days is what elementary's
    volume_anomalies does too (ADR 0024). -#}
{% test volume_anomaly(
    model,
    timestamp_column,
    training_days=var('anomaly_training_days', 14),
    min_training_days=var('anomaly_min_training_days', 5),
    sensitivity=var('anomaly_sensitivity', 3)
) %}

with clock as (

    select
        {{ simulated_now() }} as now_at,
        cast({{ simulated_now() }} at time zone 'UTC' as date) - 1 as tested_day

),

daily as (

    select
        cast({{ timestamp_column }} at time zone 'UTC' as date) as day,
        count(*) as row_count
    from {{ model }}
    where {{ timestamp_column }} < (select now_at from clock)
    group by 1

),

training as (

    select
        count(*) as training_days,
        avg(daily.row_count) as mean_rows,
        stddev_samp(daily.row_count) as stddev_rows
    from daily
    cross join clock
    where
        daily.day < clock.tested_day
        and daily.day >= clock.tested_day - {{ training_days }}

),

tested as (

    select
        clock.tested_day,
        coalesce(max(daily.row_count), 0) as row_count
    from clock
    left join daily on clock.tested_day = daily.day
    group by clock.tested_day

)

select
    tested.tested_day,
    tested.row_count,
    training.training_days,
    round(training.mean_rows, 1) as mean_rows,
    round(training.stddev_rows, 1) as stddev_rows,
    round((tested.row_count - training.mean_rows) / nullif(training.stddev_rows, 0), 2) as z_score
from tested
cross join training
where
    training.training_days >= {{ min_training_days }}
    and abs(tested.row_count - training.mean_rows) > {{ sensitivity }} * training.stddev_rows

{% endtest %}
