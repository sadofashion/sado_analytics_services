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
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`KiotViet`.`p_products_list_*`
                LIMIT
                    1000
            )
        

        
)
SELECT
    id,
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