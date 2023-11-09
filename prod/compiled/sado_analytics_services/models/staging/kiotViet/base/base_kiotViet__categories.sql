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
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`KiotViet`.`p_categories_list_*`
                LIMIT
                    1000
            )
        

        
)
SELECT
    categoryId,
    parentId,
    categoryName,
from source
WHERE
    rn_ = 1