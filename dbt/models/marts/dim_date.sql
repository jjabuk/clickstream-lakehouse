-- One row per calendar day (UTC), from the start of the simulated world onwards.

with days as (

    {{ dbt_utils.date_spine(
        datepart='day',
        start_date="cast('" ~ var('dim_date_start') ~ "' as date)",
        end_date="cast('" ~ var('dim_date_end') ~ "' as date)",
    ) }}

)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    cast(date_day as date) as date_day,
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day_of_month,
    isoyear(date_day) as iso_year,
    weekofyear(date_day) as iso_week,
    isodow(date_day) as day_of_week,
    isodow(date_day) in (6, 7) as is_weekend,
    cast(date_trunc('week', date_day) as date) as week_start_date
from days
