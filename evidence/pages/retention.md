# Retention

Visitors grouped by the ISO week (Monday) of their first visit (`fct_retention_weekly`). A cell is
the share of the cohort active in a later week; week 0 is always 100%. January 12-30 was never
loaded, so the first cohort has no weeks 1 and 2.

A visitor is a person, not a cookie: a user who logs in on the phone and on the laptop is one
visitor, and their visits before the first login count too (identity stitching, ADR 0022). Only
visitors nobody ever logged in as are counted by cookie.

```sql retention
select to_char(cohort_week, 'YYYY-MM-DD') as cohort_week, weeks_since_first_visit,
       cohort_visitors, active_visitors,
       active_visitors * 1.0 / nullif(cohort_visitors, 0) as retention
from marts.fct_retention_weekly
```

{% heatmap
  data="retention"
  x="weeks_since_first_visit"
  y="cohort_week"
  value="sum(retention)"
  value_fmt="pct0"
  x_sort="asc"
  y_sort="asc"
  x_axis_title="Weeks since first visit"
  y_axis_title="Cohort week"
  title="Share of the cohort active"
/%}

{% table data="retention" title="Active visitors per cohort" order="cohort_week" subtotals=false %}
  {% dimension value="cohort_week" title="Cohort week" /%}
  {% pivot value="weeks_since_first_visit" /%}
  {% measure value="sum(active_visitors)" fmt="num0" title="Active" /%}
{% /table %}

## Who the visitors are

How each session's person is known (`fct_sessions.identity_source`): somebody logged in during
it, the cookie is one user's device (stitched), several users log in on the cookie (a shared
computer, left unknown), or nobody ever logged in on it. From March 1 a user has a cookie per
device and often logs in only at the checkout, so stitching starts to matter.

```sql identity
select cast(session_started_at as date) as session_day, identity_source, count(*) as sessions
from marts.fct_sessions_v2
group by 1, 2
```

{% bar_chart
  data="identity"
  x="session_day"
  y="sum(sessions)"
  series="identity_source"
  stacked=true
  title="Sessions by how the person is known"
/%}

```sql people_vs_cookies
select cast(date_trunc('week', session_started_at) as date) as week,
       count(distinct cookie_id) as cookies, count(distinct person_id) as people
from marts.fct_sessions_v2
group by 1
```

{% table data="people_vs_cookies" title="Cookies and the people behind them, per week" order="week" %}
  {% dimension value="week" title="Week" /%}
  {% measure value="sum(cookies)" fmt="num0" title="Cookies" /%}
  {% measure value="sum(people)" fmt="num0" title="People" /%}
{% /table %}

