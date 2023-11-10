{% macro source(
        source_name,
        table_name
    ) %}
    {% set original_source = builtins.source(
        source_name,
        table_name
    ) %}

    {% if (not 'ignore' in config.get('tags') and config.get('materialized') in ['table','view']) %}
        {% set limit_result %}
                (
                    SELECT
                        *
                    FROM
                        {{ original_source }}
                    LIMIT
                        1000
                )
        {% endset %}
        {% do return(limit_result) %}
    {% else %}
        {% do return(original_source) %}
    {% endif %}
{% endmacro %}
