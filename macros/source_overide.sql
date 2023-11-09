{% macro source(
        source_name,
        table_name
    ) %}
    {% set original_source = builtins.source(
        source_name,
        table_name
    ) %}
    {% if target.name == 'prod' %}
        {% do return(original_source) %}
    {% else %}
        {% set limit_result %}
        {% if (config.get('materialized') == 'incremental') %}
            {{ original_source }}
        {% else %}
            (
                SELECT
                    *
                FROM
                    {{ original_source }}
                LIMIT
                    1000
            )
        {% endif %}

        {% endset %}
        {% do return(limit_result) %}
    {% endif %}
{% endmacro %}
