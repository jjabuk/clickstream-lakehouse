{# Match the type of the timestamptz columns the snapshot derives from `updated_at`. #}
{% macro duckdb__snapshot_get_time() %}
    cast(current_timestamp as timestamp with time zone)
{% endmacro %}
a