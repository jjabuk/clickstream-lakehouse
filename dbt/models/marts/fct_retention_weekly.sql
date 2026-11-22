-- Weekly cohort retention. A visitor (cookie) belongs to the ISO week of their first human page
-- view; the table counts how many of each cohort were active in every later week.
--
-- The cohort is recomputed on every run, so a late event that predates a visitor's first known
-- visit moves them to the earlier cohort, as it should.

with visitor_weeks as (

    select distinct
        cookie_id,
        cast(date_trunc('week', occurred_at at time zone 'UTC') as date) as activity_week
    from {{ ref('fct_page_views') }}

),

cohorts as (

    select
        cookie_id,
        activity_week,
        min(activity_week) over (partition by cookie_id) as cohort_week
    from visitor_weeks

),

cohort_sizes as (

    select
        cohort_week,
        count(distinct cookie_id) as cohort_visitors
    from cohorts
    group by cohort_week

)

select
    {{ dbt_utils.generate_surrogate_key(['cohorts.cohort_week', 'cohorts.activity_week']) }}
        as retention_key,
    cohorts.cohort_week,
    cohorts.activity_week,
    cast(date_diff('week', cohorts.cohort_week, cohorts.activity_week) as integer)
        as weeks_since_first_visit,
    cohort_sizes.cohort_visitors,
    count(*) as active_visitors
from cohorts
inner join cohort_sizes on cohorts.cohort_week = cohort_sizes.cohort_week
group by cohorts.cohort_week, cohorts.activity_week, cohort_sizes.cohort_visitors
