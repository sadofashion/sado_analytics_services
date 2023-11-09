WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'kiotViet',
            'p_customers_list_*'
        ) }}
    UNION ALL
    SELECT
        *
    FROM
        {{ source(
            'kiotViet',
            'p_webhook_customer_update'
        ) }}
),
raw_ AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                _batched_at DESC,
                modifiedDate DESC
        ) AS rn_
    FROM
        source
)
SELECT
    id,
    code,
    NAME,
    gender,
    birthDate,
    contactNumber,
    branchId,
    TYPE,
    raw_.groups,
    debt,
    totalInvoiced,
    totalPoint,
    totalRevenue,
    rewardPoint,
    createdDate,
    modifiedDate,
FROM
    raw_
WHERE
    rn_ = 1
