-- One row per session, aggregated from fct_page_views. Version 2: the campaign column is
-- `campaign`, the name of the semantic layer's dimension; version 1 called it `utm_campaign`
-- (ADR 0025). dbt v2 points the relation fct_sessions at the latest version.
--
-- A plain table on purpose. A late page view can bridge two sessions into one, and then the
-- later session's id disappears. An incremental merge on session_id cannot delete the vanished
-- row and would leave a phantom session; rebuilding from the (incremental) fact is correct by
-- construction and cheap, since it is one aggregation.
--
-- Attributes that can vary inside a session are taken deterministically (any_value would make
-- two builds of the same data disagree): device and country from the entry page view, campaign
-- and user from the first page view that has one. A visitor who logs in mid-session is attributed
-- for the whole session; the unknown member stands in when nobody logs in.
--
-- Identity (ADR 0022): `user_id` is who logged in during the session. `identified_user_id` adds
-- the stitched user of the cookie (int_identity__cookie_to_user) for a session nobody logged in
-- on, so a visit before a user's first login, or on their phone while they log in on the laptop,
-- is theirs too. `person_id` is the visitor to count: the identified user, or the cookie when
-- nobody is known. Being a plain table makes the stitching retroactive at no cost.
--
-- The funnel steps are the closed funnel of int_sessions__funnel_steps (cart only after a
-- product view, checkout only after that cart view), the one definition every mart and the
-- semantic layer use. A cart page opened straight from an e-mail link is not a cart step.

with sessions as (

    select
        session_id,
        any_value(cookie_id) as cookie_id,
        arg_min(user_id, page_number_in_session) filter (where user_id is not null) as user_id,
        coalesce(
            arg_min(user_key, page_number_in_session) filter (where user_id is not null),
            md5('unknown user')
        ) as user_key,
        min(occurred_at) as session_started_at,
        max(occurred_at) as session_ended_at,
        cast(strftime(min(occurred_at) at time zone 'UTC', '%Y%m%d') as integer) as date_key,
        epoch(max(occurred_at)) - epoch(min(occurred_at)) as duration_seconds,
        count(*) as page_views,
        count(*) = 1 as is_bounce,
        arg_min(page_type, page_number_in_session) as entry_page_type,
        arg_max(page_type, page_number_in_session) as exit_page_type,
        count(*) filter (where page_type = 'product') as product_views,
        arg_min(device, page_number_in_session) as device,
        arg_min(country, page_number_in_session) as country,
        arg_min(utm_campaign, page_number_in_session) filter (where utm_campaign is not null)
            as campaign,
        max(received_at) as last_received_at
    from {{ ref('fct_page_views') }}
    group by session_id

)

select
    sessions.*,
    steps.reached_product,
    steps.reached_cart,
    steps.reached_checkout,
    coalesce(sessions.user_id, cookie_owner.user_id) as identified_user_id,
    case
        when sessions.user_id is not null then 'login'
        when cookie_owner.user_id is not null then 'stitched'
        when cookie_owner.is_shared_device then 'shared_device'
        else 'anonymous'
    end as identity_source,
    coalesce(
        'user:' || cast(coalesce(sessions.user_id, cookie_owner.user_id) as varchar),
        'cookie:' || sessions.cookie_id
    ) as person_id
from sessions
inner join {{ ref('int_sessions__funnel_steps') }} as steps
    on sessions.session_id = steps.session_id
left join {{ ref('int_identity__cookie_to_user') }} as cookie_owner
    on sessions.cookie_id = cookie_owner.cookie_id
