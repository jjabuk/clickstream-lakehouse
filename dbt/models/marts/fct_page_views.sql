{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
    )
}}

-- Page views of humans, sessionized. Incremental by merge on event_id, with a lookback window
-- for late events (ADR 0004). Rows inside the window are recomputed, because a late event can
-- change which session its neighbours belong to.
--
-- Why rows before the window never need recomputing: any event that arrives now occurred no
-- earlier than the window start (lateness is bounded by lookback_hours), so it cannot lie before
-- the first event of an older session nor between two older sessions.

with
{% if is_incremental() %}

watermark as (

    select coalesce(max(received_at), timestamptz '1970-01-01') as received_at from {{ this }}

),

window_start as (

    select received_at - to_hours({{ var('lookback_hours') }}) as occurred_at from watermark

),

{% endif %}

page_views as (

    select
        event_id,
        cookie_id,
        user_id,
        occurred_at,
        received_at,
        page_type,
        page_url,
        product_id,
        device,
        country,
        utm_campaign
    from {{ ref('stg_clickstream__events') }}
    where event_type = 'page_view' and not is_bot
    {% if is_incremental() %}
        -- anything that arrived after the last run, and everything recent enough to be affected
        and (
            received_at > (select watermark.received_at from watermark)
            or occurred_at >= (select window_start.occurred_at from window_start)
        )
    {% endif %}

),

sessionization_input as (

    select
        event_id,
        cookie_id,
        occurred_at
    from page_views
    {% if is_incremental() %}

    union all

    -- Context: older rows, already in the table, that the first views of the window follow.
    select event_id, cookie_id, occurred_at
    from {{ this }}
    where
        occurred_at < (select window_start.occurred_at from window_start)
        and occurred_at >= (select window_start.occurred_at from window_start) - to_hours({{ var('session_context_hours') }})
        and event_id not in (select page_views.event_id from page_views)
    {% endif %}

),

flagged as (

    select
        event_id,
        cookie_id,
        occurred_at,
        cast(
            coalesce(
                occurred_at - lag(occurred_at) over visitor
                > to_minutes({{ var('session_timeout_minutes') }}),
                true
            ) as integer
        ) as is_new_session
    from sessionization_input
    window visitor as (partition by cookie_id order by occurred_at, event_id)

),

numbered as (

    select
        *,
        sum(is_new_session) over (
            partition by cookie_id order by occurred_at, event_id rows unbounded preceding
        ) as session_number
    from flagged

),

sessions as (

    select
        event_id,
        min(occurred_at) over (partition by cookie_id, session_number) as session_started_at,
        row_number() over (
            partition by cookie_id, session_number order by occurred_at, event_id
        ) as page_number_in_session
    from numbered

)

select
    page_views.event_id,
    md5(
        page_views.cookie_id || '|' || cast(epoch_ms(sessions.session_started_at) as varchar)
    ) as session_id,
    sessions.session_started_at,
    sessions.page_number_in_session,
    page_views.cookie_id,
    page_views.user_id,
    coalesce(dim_user.user_key, md5('unknown user')) as user_key,
    page_views.occurred_at,
    cast(strftime(page_views.occurred_at at time zone 'UTC', '%Y%m%d') as integer) as date_key,
    page_views.received_at,
    page_views.page_type,
    {{ dbt_utils.generate_surrogate_key(['page_views.page_url']) }} as page_key,
    page_views.page_url,
    page_views.product_id,
    page_views.device,
    page_views.country,
    page_views.utm_campaign
from page_views
inner join sessions on page_views.event_id = sessions.event_id
left join {{ ref('dim_user') }} as dim_user
    on
        page_views.user_id = dim_user.user_id
        and page_views.occurred_at >= dim_user.effective_from
        and page_views.occurred_at < dim_user.effective_to
