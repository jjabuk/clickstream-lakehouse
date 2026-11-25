# Data health

Is the data on this dashboard complete, and did the pipeline get it here (ADR 0026)? The marts'
SLA is checked against the simulated clock after every replayed day: a mart more than 6 hours
behind warns, more than 18 hours fails. Alerts also come from failed tasks, such as a delivery the
audit rejected, and from runs still going 4 minutes after they were queued.

```sql published
select simulated_now, published_at from marts._published
```

{% table data="published" title="Last publish" /%}

```sql alerts
select raised_at, simulated_day, level, kind, subject, detail
from ops.pipeline_alerts
order by raised_at desc
limit 50
```

{% table data="alerts" title="Latest alerts" /%}

An empty table means every replayed day reached the marts on time. `make alerts` shows the same
list in the terminal.
