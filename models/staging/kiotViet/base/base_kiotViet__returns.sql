WITH source AS (
    SELECT
        *
    EXCEPT(returnDetails),
        returnDetails
    FROM
        {{ source(
            'kiotViet',
            'p_returns_list_*'
        ) }}
    UNION ALL
    SELECT
        *
    EXCEPT(return_details),
        return_details AS returnDetails
    FROM
        {{ source(
            'kiotViet',
            'p_returns_list2_*'
        ) }}
),
raw_ AS (
    {{ dbt_utils.deduplicate(
        relation = 'source',
        partition_by = 'id',
        order_by = "modifiedDate DESC,_batched_at desc",
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
    "return" AS transaction_type
FROM
    raw_
