WITH source AS (
    SELECT
        * except(returnDetails),
        returnDetails
    FROM
        {{ source(
            'kiotViet',
            'p_returns_list_*'
        ) }}
        union all 
        SELECT
        * except(return_details),
        return_details as returnDetails
    FROM
        {{ source(
            'kiotViet',
            'p_returns_list2_*'
        ) }}

),
raw_ as (
    select * ,
    ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                _batched_at DESC,
                modifiedDate DESC
        ) AS rn_
        from source
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
    raw_
WHERE
    rn_ = 1
