SELECT
    id AS transactionId,
    code AS transactionCode,
    purchaseDate AS transactionDate,
    branchId,
    soldById AS employeeId,
    customerId,
    orderCode AS referenceTransactionId,
    total,
    totalPayment,
    statusValue AS transactionStatus,
    createdDate,
    modifiedDate,
    discountRatio,
    discount,
FROM
    {{ ref('base_kiotViet__invoices') }}
