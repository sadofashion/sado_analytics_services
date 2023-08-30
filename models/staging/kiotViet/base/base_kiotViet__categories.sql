WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY categoryId
            ORDER BY
                modifiedDate DESC,
                _batched_at DESC
        ) AS rn_
    FROM
        {{ source(
            'kiotViet',
            'p_categories_list_*'
        ) }}
)
SELECT
    categoryId,
    parentId,
    categoryName,
from source
WHERE
    rn_ = 1
