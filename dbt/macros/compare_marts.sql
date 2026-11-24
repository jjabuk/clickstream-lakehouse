{#
    Compares every mart with the same mart in another warehouse, attached as `before_database`
    (target `compare`, make marts-compare). Used to show that swapping the ingest from landing
    files to Kafka changes nothing downstream (ADR 0005). audit_helper's compare_relations
    summarises rows found in both, in only one, or in the other; anything but "both" is a
    difference. Fails if any mart differs.
#}
{% macro compare_marts(before_database='before') %}
    {% set differing = [] %}
    {% for node in graph.nodes.values()
        if node.resource_type == 'model' and node.fqn[1] == 'marts' %}
        {% set after = api.Relation.create(
            database=target.database, schema=node.schema, identifier=node.alias) %}
        {% set before = api.Relation.create(
            database=before_database, schema=node.schema, identifier=node.alias) %}
        {% set summary = run_query(audit_helper.compare_relations(
            a_relation=before, b_relation=after, summarize=true)) %}
        {% set only_one = [] %}
        {% set ns = namespace(both=0) %}
        {% for row in summary.rows %}
            {% if row['in_a'] and row['in_b'] %}
                {% set ns.both = row['count'] %}
            {% else %}
                {% do only_one.append(row['count']) %}
            {% endif %}
        {% endfor %}
        {% set state = 'identical' if not only_one else 'DIFFERENT' %}
        {% do log(node.name ~ ': ' ~ ns.both ~ ' rows in both, ' ~ (only_one | sum)
            ~ ' in only one: ' ~ state, info=true) %}
        {% if only_one %}{% do differing.append(node.name) %}{% endif %}
    {% endfor %}
    {% if differing %}
        {{ exceptions.raise_compiler_error('marts differ: ' ~ differing | join(', ')) }}
    {% endif %}
{% endmacro %}
