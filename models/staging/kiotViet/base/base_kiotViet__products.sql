WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                modifiedDate DESC,
                _batched_at DESC
        ) AS rn_
    FROM
        {{ source(
            'kiotViet',
            'p_products_list_*'
        ) }}
)
SELECT
    id AS productId,
    categoryId,
    fullName,
    code,
    tradeMarkName,
    isActive,
    type,
    attributes
from source
WHERE
    rn_ = 1
