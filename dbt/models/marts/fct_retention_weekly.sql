-- Weekly cohort retention. A person belongs to the ISO week of their first human session; the
-- table counts how many of each cohort were active in every later week.
--
-- The person is fct_sessions.person_id (ADR 0022): a user on all their devices is one person, and
-- their visits before the first login count from the week they happened. By cookie, every new
-- device would look like a new visitor who never came back, and retention would read too low.
--
-- The cohort is recomputed on every run, so a late event that predates a person's first known
-- visit, or a new login that stitches an older cookie, moves them to the earlier cohort.

with visitor_weeks as (

    select distinct
        person_id,
        cast(date_trunc('week', session_started_at at time zone 'UTC') as date) as activity_week
    from {{ ref('fct_sessions') }}

),

cohorts as (

    select
        person_id,
        activity_week,
        min(activity_week) over (partition by person_id) as cohort_week
    from visitor_weeks

),

cohort_sizes as (

    select
        cohort_week,
        count(distinct person_id) as cohort_visitors
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
