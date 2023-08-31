SELECT
    id AS transaction_id,
    code AS transactionCode,
    purchaseDate AS transaction_date,
    branchId,
    soldById AS employeeId,
    customerId,
    orderCode AS referencetransaction_id,
    total,
    totalPayment,
    statusValue AS transactionStatus,
    createdDate,
    modifiedDate,
    discountRatio,
    discount,
FROM
    {{ ref('base_kiotViet__invoices') }}
