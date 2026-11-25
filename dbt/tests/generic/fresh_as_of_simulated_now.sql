{#-
    Freshness of a mart against the simulated clock (ADR 0026). dbt's source freshness measures
    from the wall clock, which says nothing when the shop's days are simulated.

    `as_of` comes from `--vars '{as_of: ...}'` or from SIMULATED_NOW (make dbt-build sets it from
    the clock file); without either the test passes, since there is no "now" to compare with.
    The latest value of the column is compared with `as_of`: a `period: day` column (a date)
    counts as complete until the end of that day. The test returns one row per threshold the lag
    passes, so `warn_if: >0` warns past `warn_after_hours` and `error_if: >1` fails past
    `error_after_hours`. An empty model is as stale as can be.
-#}
{% test fresh_as_of_simulated_now(model, column_name, warn_after_hours, error_after_hours, period='timestamp') %}
    {{ config(warn_if='>0', error_if='>1', tags=['freshness']) }}
    {%- set as_of = var('as_of', env_var('SIMULATED_NOW', '')) -%}
    {#- A placeholder keeps the SQL valid when there is no clock; the filter then returns nothing. -#}
    {%- set as_of_sql = as_of if as_of else '1970-01-01T00:00:00+00:00' -%}

    {%- if period == 'day' -%}
        {%- set latest = "(cast(max(" ~ column_name ~ ") as timestamp) + interval 1 day) at time zone 'UTC'" -%}
    {%- else -%}
        {%- set latest = "max(" ~ column_name ~ ")" -%}
    {%- endif %}

    with latest as (
        select coalesce({{ latest }}, timestamptz '1970-01-01 00:00:00+00') as latest_at
        from {{ model }}
    ),

    lag as (
        select
            latest_at,
            cast('{{ as_of_sql }}' as timestamptz) as as_of,
            (epoch(cast('{{ as_of_sql }}' as timestamptz)) - epoch(latest_at)) / 3600 as lag_hours
        from latest
    ),

    thresholds as (
        select * from (
            values ('warn', {{ warn_after_hours }}), ('error', {{ error_after_hours }})
        ) as t (level, after_hours)
    )

    select lag.*, thresholds.level, thresholds.after_hours
    from lag
    cross join thresholds
    where
        {{ 'true' if as_of else 'false' }}
        and lag.lag_hours > thresholds.after_hours
{% endtest %}
