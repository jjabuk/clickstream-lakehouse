# Funnel

A session counts at a step only if it reached the steps before it (`fct_funnel_daily`). Every
rate is a ratio of the summed counts, so it stays correct at any grain and filter.

{% row %}
{% dropdown id="device" data="marts.fct_funnel_daily" value_column="device" title="Device" multiple=true /%}
{% dropdown id="campaign" data="marts.fct_funnel_daily" value_column="utm_campaign" title="Campaign" multiple=true /%}
{% /row %}

```sql funnel_steps
select '1. session' as step, sum(sessions) as sessions, device, utm_campaign
from marts.fct_funnel_daily group by device, utm_campaign
union all
select '2. product', sum(product_sessions), device, utm_campaign
from marts.fct_funnel_daily group by device, utm_campaign
union all
select '3. cart', sum(cart_sessions), device, utm_campaign
from marts.fct_funnel_daily group by device, utm_campaign
union all
select '4. checkout', sum(checkout_sessions), device, utm_campaign
from marts.fct_funnel_daily group by device, utm_campaign
```

{% funnel_chart
  data="funnel_steps"
  category="step"
  value="sum(sessions)"
  order="step"
  filters=["device", "campaign"]
  show_percent=true
  title="Sessions reaching each step"
/%}

{% line_chart
  metric=["product_rate", "cart_rate", "checkout_rate"]
  filters=["device", "campaign"]
  y_fmt="pct0"
  handle_missing="gaps"
  title="Step rates per day"
/%}

## Across sessions

The rates above count within one session. A visitor who views a product today and buys three
days later shows up only here: of the visitors who viewed a product on a day, the share that
reached checkout within 7 days, in any session. The last 7 days of data are left out, because
their window is still open and they can still convert.

{% row %}
{% big_value
  data="marts.fct_visitor_conversion_daily"
  value="avg(case when is_converted_7d then 1.0 else 0.0 end)"
  where="is_window_complete"
  fmt="pct1"
  title="Product view to checkout in 7 days"
/%}
{% big_value metric="checkout_rate" title="Within the session" /%}
{% /row %}

{% bar_chart
  metric="visitor_product_to_checkout_7d"
  x="product_week"
  where="is_window_complete"
  y_fmt="pct0"
  title="Product view to checkout within 7 days, by week of the product view"
/%}

January 12-30 was never loaded, so visitors from the week of January 5 had no data to convert in
for most of their window. Their rate is low for that reason, not because they bought less.

{% table filters=["device", "campaign"] title="By device and campaign" %}
  {% dimension value="device" title="Device" /%}
  {% dimension value="utm_campaign" title="Campaign" /%}
  {% measure metric="sessions" /%}
  {% measure metric="bounce_rate" /%}
  {% measure metric="product_rate" /%}
  {% measure metric="cart_rate" /%}
  {% measure metric="checkout_rate" /%}
{% /table %}

`utm_campaign` starts on 2026-02-01 (schema evolution in bronze), so the days before it show
`(none)`.
