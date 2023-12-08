WITH source AS (
    {{ dbt_utils.deduplicate(relation = source('pancake', 'tags'), partition_by = 'id', order_by = "_batched_at desc",) }}
)
SELECT
    source.id AS tag_id,
    source.text AS tag_value,
FROM
    source
