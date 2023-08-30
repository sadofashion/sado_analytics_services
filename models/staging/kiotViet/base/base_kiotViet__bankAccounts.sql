WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                _batched_at DESC
        ) AS rn_
    FROM
        {{ source(
            'kiotViet',
            'p_bankAccounts_list_*'
        ) }}
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
