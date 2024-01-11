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
    {{ dbt_utils.deduplicate(relation = 'source', partition_by = 'id', order_by = "_batched_at desc",) }}
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