# Retention

Visitors grouped by the ISO week (Monday) of their first visit (`fct_retention_weekly`). A cell is
the share of the cohort active in a later week; week 0 is always 100%. January 12-30 was never
loaded, so the first cohort has no weeks 1 and 2.

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
