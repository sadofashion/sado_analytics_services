WITH source AS (

    {{ dbt_utils.deduplicate(relation = source(
            'kiotViet',
            'p_categories_list_*'
        ), partition_by = 'categoryId', order_by = "modifiedDate DESC,_batched_at desc",) }}
)
SELECT
    categoryId,
    parentId,
    categoryName,
from source
