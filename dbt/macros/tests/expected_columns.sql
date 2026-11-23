{#- Schema drift: one row per column that is in the table but not expected, or expected but
    missing. `optional_columns` may be absent, for columns that schema evolution adds on a later
    day (utm_campaign). Unlike elementary's schema_changes, it keeps no history: the expected
    schema is written down here, next to the source. -#}
{% test expected_columns(model, columns, optional_columns=[]) %}

{%- set expected = columns | map('lower') | list -%}
{%- set optional = optional_columns | map('lower') | list -%}
{%- if execute -%}
    {%- set actual = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower') | list -%}
{%- else -%}
    {%- set actual = expected -%}
{%- endif -%}
{%- set issues = [] -%}
{%- for column in actual if column not in expected and column not in optional -%}
    {%- do issues.append((column, 'not expected')) -%}
{%- endfor -%}
{%- for column in expected if column not in actual -%}
    {%- do issues.append((column, 'missing')) -%}
{%- endfor %}

select *
from (
    values
    {%- for column, issue in issues %}
        ('{{ column }}', '{{ issue }}'),
    {%- endfor %}
        (null, null)
) as drift (column_name, issue)
where column_name is not null

{% endtest %}
