WITH source AS (
    SELECT
        *
    EXCEPT(purchaseOrderDetails),
        purchaseOrderDetails
    FROM
        {{ source(
            'kiotViet',
            'p_purchaseorders_list_*'
        ) }}
    UNION ALL
    SELECT
        *
    EXCEPT(purchaseOrder_details),
        purchaseOrder_details AS purchaseOrderDetails
    FROM
        {{ source(
            'kiotViet',
            'p_purchaseorders_list2_*'
        ) }}
),
raw_ AS (
    {{ dbt_utils.deduplicate(
        relation = 'source',
        partition_by = 'id',
        order_by = "_batched_at desc",
    ) }}
)
SELECT
    id,
    code,
    purchaseDate,
    branchId,
    purchaseById,
    supplierId,
    supplierName,
    supplierCode,
    partnerType,
    total,
    totalPayment,
    discount,
    discountRatio,
    status,
    createdDate,
    purchaseOrderDetails,
FROM
    raw_
WHERE
    rn_ = 1
