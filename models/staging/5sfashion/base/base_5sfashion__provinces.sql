WITH source AS (
    {{dbt_utils.deduplicate(source=source(
            '5sfashion',
            'provinces'
        ),partition_by='_id', order_by='updated_at DESC,_batched_at DESC'
        )}}
)
SELECT
    *
EXCEPT(rn_, _batched_at)
FROM
    source
