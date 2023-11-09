WITH source AS (
    SELECT
        * except(returnDetails),
        returnDetails
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`KiotViet`.`p_returns_list_*`
                LIMIT
                    1000
            )
        

        
        union all 
        SELECT
        * except(return_details),
        return_details as returnDetails
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`KiotViet`.`p_returns_list2_*`
                LIMIT
                    1000
            )
        

        

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