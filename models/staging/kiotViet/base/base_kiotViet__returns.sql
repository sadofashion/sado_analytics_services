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
            'p_returns_list_*'
        ) }}
)
SELECT
    id,
    code,
    invoiceId,
    returnDate,
    branchId,
    receivedById,
    customerId,
    returnTotal,
    returnDiscount,
    totalPayment,
    returnFee,
    returnFeeRatio,
    saleChannelId,
    statusValue,
    createdDate,
    modifiedDate,
    payments,
    returnDetails,
    "return" as transaction_type
FROM
    source
WHERE
    rn_ = 1
