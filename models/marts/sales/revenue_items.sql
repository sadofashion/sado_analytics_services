SELECT
    transaction_id,
    transactionCode,
    transaction_date,
    branchId,
    customerId,
    employeeId,
    productId,
    quantity,
    price,
    discountRatio,
    discount,
    subTotal
FROM
    {{ ref('stg_kiotviet__invoicedetails') }}
    where transactionStatus = "Hoàn thành"
    union ALL
SELECT
    transaction_id,
    transactionCode,
    transaction_date,
    branchId,
    customerId,
    employeeId,
    productId,
    quantity,
    price,
    discountRatio,
    discount,
    subTotal
FROM
    {{ ref('stg_kiotviet__returndetails') }}
    where transactionStatus = "Đã trả"