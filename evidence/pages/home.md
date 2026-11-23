# Clickstream lakehouse

Synthetic web shop traffic, loaded day by day into Iceberg and modelled with dbt. This
dashboard reads the marts that `make marts-publish` copies to Postgres after each build.

```sql published
select simulated_now, published_at from marts._published
```

{% table data="published" title="Last publish" /%}

{% row %}
{% big_value metric="sessions" title="Sessions" /%}
{% big_value metric="bounce_rate" /%}
{% big_value metric="checkout_rate" /%}
{% big_value metric="bot_share" /%}
{% /row %}

{% line_chart metric="sessions" handle_missing="gaps" title="Human sessions per day" /%}

January 12-30 was never loaded, hence the gap. The low points on January 9 and 30 are late
events: they arrived with the next day's file, but their own day's file is not in bronze.

- [Funnel](/funnel): product, cart and checkout by device and campaign
- [Retention](/retention): weekly cohorts by first visit
- [Traffic](/traffic): every event that reached bronze, human and bot
