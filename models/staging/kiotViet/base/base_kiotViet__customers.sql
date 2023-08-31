WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                _batched_at DESC,
                modifiedDate DESC
        ) AS rn_
    FROM
        {{ source(
            'kiotViet',
            'p_customers_list_*'
        ) }}
)
SELECT
    id,
    code,
    name,
    gender,
    birthDate,
    contactNumber,
    branchId,
    type,
    source.groups,
    debt,
    totalInvoiced,
    totalPoint,
    totalRevenue,
    rewardPoint
FROM
    source
WHERE
    rn_ = 1
