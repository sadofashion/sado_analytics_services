SELECT
    invoices.transaction_id,
    invoices.transactionCode,
    invoices.transaction_date,
    CAST(
        NULL AS int64
    ) AS referencetransaction_id,
    invoices.branchId,
    invoices.customerId,
    invoices.employeeId,
    invoices.total,
    invoices.totalPayment,
    invoices.discount,
    invoices.discountRatio,
    CAST(
        NULL AS int64
    ) AS returnFee,
    "invoice" AS transaction_type,
FROM
    {{ ref('stg_kiotviet__invoices') }}
    invoices
WHERE
    invoices.transactionStatus = 'Hoàn thành'
UNION ALL
SELECT
    returns.transaction_id,
    returns.transactionCode,
    returns.transaction_date,
    returns.referencetransaction_id,
    returns.branchId,
    returns.customerId,
    returns.employeeId,
    - returns.total as total,
    returns.totalPayment,
    returns.returnDiscount,
    CAST(
        NULL AS float64
    ) discountRatio,
    returns.returnFee,
    "return" AS transaction_type
FROM
    {{ ref('stg_kiotviet__returns') }}
    returns
WHERE
    returns.transactionStatus = 'Đã trả'
