WITH source AS (
    {{ dbt_utils.deduplicate(relation = source('pancake', 'tags'), partition_by = 'id', order_by = "_batched_at desc",) }}
)

SELECT
distinct
    source.id AS tag_id,
    source.text AS tag_value,
    ref.category
FROM
    source
    left join {{ref("stg_gsheet__pancake_tags")}} ref on source.text = ref.tag_value 
