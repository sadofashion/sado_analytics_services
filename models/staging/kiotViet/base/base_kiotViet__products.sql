WITH source AS (
    {{ dbt_utils.deduplicate(
        relation = source(
            'kiotViet',
            'p_products_list_*'
        ),
        partition_by = 'id',
        order_by = "modifiedDate DESC,_batched_at desc",
    ) }}
)
SELECT
    id,
    categoryId,
    fullName,
    code,
    tradeMarkName,
    isActive,
    TYPE,
    attributes
FROM
    source
WHERE
    rn_ = 1
