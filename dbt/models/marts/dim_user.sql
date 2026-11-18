-- Type 2 user dimension built on the snapshot, plus one "unknown" member for anonymous visits.
--
-- effective_from of the first observed version is pushed back to 1900: the snapshot did not
-- exist before its first run, so the earliest known attributes are the best available for
-- older events (the alternative, an unknown member, would drop attributes we may well have).

with versions as (

    select
        dbt_scd_id as user_key,
        user_id,
        country,
        plan,
        coalesce(
            lag(dbt_valid_to) over (partition by user_id order by dbt_valid_from) is null,
            true
        ) as is_first_version,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('snap_users') }}

)

select
    user_key,
    user_id,
    country,
    plan,
    case
        when is_first_version then timestamptz '1900-01-01 00:00:00+00'
        else dbt_valid_from
    end as effective_from,
    coalesce(dbt_valid_to, timestamptz '9999-12-31 00:00:00+00') as effective_to,
    dbt_valid_to is null as is_current
from versions

union all

select
    md5('unknown user') as user_key,
    null as user_id,
    'unknown' as country,
    'unknown' as plan,
    timestamptz '1900-01-01 00:00:00+00' as effective_from,
    timestamptz '9999-12-31 00:00:00+00' as effective_to,
    true as is_current
