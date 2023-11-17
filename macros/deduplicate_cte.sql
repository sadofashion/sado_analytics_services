{% macro deduplicate_cte(
        cte,
        partition_fields,
        last_updated_fields
    ) %}
    deduplicate_cte AS (
        SELECT
            *,
            ROW_NUMBER() over (
                PARTITION BY {{ partition_fields | join(',') }}
                ORDER BY
                    {{ last_updated_fields | join(' desc,') }} DESC
            ) AS rn_
        FROM
            {{ cte | string() }}
    )
SELECT
    *
EXCEPT(rn_)
FROM
    deduplicate_cte
WHERE
    rn_ = 1
{% endmacro %}
