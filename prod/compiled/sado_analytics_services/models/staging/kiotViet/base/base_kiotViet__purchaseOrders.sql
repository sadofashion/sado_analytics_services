WITH source AS (
    SELECT
        *
    EXCEPT(purchaseOrderDetails),
        purchaseOrderDetails
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`KiotViet`.`p_purchaseorders_list_*`
                LIMIT
                    1000
            )
        

        
    UNION ALL
    SELECT
        *
    EXCEPT(purchaseOrder_details),
        purchaseOrder_details AS purchaseOrderDetails
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`KiotViet`.`p_purchaseorders_list2_*`
                LIMIT
                    1000
            )
        

        
),
raw_ AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                _batched_at DESC
        ) AS rn_
        from source
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