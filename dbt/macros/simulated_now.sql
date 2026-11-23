{#- "Now" for the tests that compare data with the clock. Time in the data is simulated, so this is
    the simulated clock: the end of the latest arrival day loaded, written by `make day` and the
    Airflow DAG and passed as SIMULATED_NOW. Without it, the wall clock. -#}
{% macro simulated_now() -%}
    {%- set now = env_var('SIMULATED_NOW', '') -%}
    {%- if now -%}
        cast('{{ now }}' as timestamp with time zone)
    {%- else -%}
        current_timestamp
    {%- endif -%}
{%- endmacro %}
