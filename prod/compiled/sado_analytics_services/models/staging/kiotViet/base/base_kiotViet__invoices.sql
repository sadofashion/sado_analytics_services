WITH source AS (
    SELECT
        * except(invoiceDelivery,invoiceDetails),
        invoiceDetails
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`KiotViet`.`p_invoices_list_*`
                LIMIT
                    1000
            )
        

         invoice1
    union all 
    select * except(invoiceDelivery,invoice_details),
    invoice_details as invoiceDetails
    from 
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`KiotViet`.`p_invoices_list2_*`
                LIMIT
                    1000
            )
        

         
    UNION ALL
    SELECT
        * except(invoiceDelivery,invoiceDetails),
        invoiceDetails
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`KiotViet`.`p_webhook_invoice_update`
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
                _batched_at DESC,
                modifiedDate DESC
        ) AS rn_
    FROM
        source
)
SELECT
    id,
    uuid,
    code,
    purchaseDate,
    branchId,
    soldById,
    customerId,
    orderCode,
    total,
    totalPayment,
    statusValue,
    COALESCE(first_value(createdDate ignore nulls) over (partition by id order by _batched_at asc), 
    first_value(_batched_at) over (partition by id order by _batched_at asc)
     ) createdDate ,
    modifiedDate,
    discountRatio,
    discount,
    payments,
    invoiceDetails,
    "invoice" as transaction_type,
FROM
    raw_
WHERE
    rn_ = 1