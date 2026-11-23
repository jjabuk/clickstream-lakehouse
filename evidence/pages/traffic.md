# Traffic

Every event that reached bronze, before the marts drop bot sessions (`fct_traffic_daily`).

{% row %}
{% big_value metric="events" /%}
{% big_value metric="bot_events" /%}
{% big_value metric="bot_share" /%}
{% /row %}

{% bar_chart
  metric="events"
  series="visitor"
  stacked=true
  handle_missing="gaps"
  title="Events per day"
/%}

{% table data="marts.fct_traffic_daily" title="Event types" %}
  {% dimension value="case when is_bot then 'bot' else 'human' end" title="Visitor" /%}
  {% measure value="sum(events)" fmt="num0" title="Events" /%}
  {% measure value="sum(page_view_events)" fmt="num0" title="Page views" /%}
  {% measure value="sum(add_to_cart_events)" fmt="num0" title="Add to cart" /%}
{% /table %}
