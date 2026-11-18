{{ config(severity='warn') }}

-- A human event that arrives later than the lookback window cannot be reprocessed against its
-- neighbours (ADR 0004), so its session may be stale. This reports them; it does not fail the
-- build, because the data is still valid, only possibly less exact.
select
    event_id,
    occurred_at,
    received_at,
    arrival_lag_seconds / 3600 as arrival_lag_hours
from {{ ref('stg_clickstream__events') }}
where not is_bot and arrival_lag_seconds > {{ var('lookback_hours') }} * 3600
