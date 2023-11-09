WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                _batched_at DESC
        ) AS rn_
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`KiotViet`.`p_bankAccounts_list_*`
                LIMIT
                    1000
            )
        

        
)
SELECT
    id,
    bankName,
    accountNumber,
    description,
    createdDate,
    modifiedDate
from source
WHERE
    rn_ = 1