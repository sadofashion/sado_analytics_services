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
            'p_purchaseorders_list_*'
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
    source
WHERE
    rn_ = 1
