{% macro union_relations(
        relations,
        exclude,
        include
    ) %}
    {% if target.name != 'prod' %}
        {% set overide_union %}
        {% for relation in relations %}
        SELECT
            {% if include is defined %}
                {{ include | join(',') }}
            {% else %}
                * {% if exclude is defined %}
                EXCEPT
                    (
                        {{ exclude | join(',') }}
                    )
                {% endif %}
            {% endif %}
        FROM
            {{ relation }}
            {{ 'union all' if not loop.last }}
        {% endfor %}

        {% endset %}
        {% do return(overide_union) %}
    {% else %}
        {% set dbt_utils_unions = dbt_utils.union_relations(
            relations = relations,
            exclude = exclude,
            include = include
        ) %}
        {% do return(dbt_utils_unions) %}
    {% endif %}
{% endmacro %}
