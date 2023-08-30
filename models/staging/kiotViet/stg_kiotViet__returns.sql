SELECT
    id AS transactionId,
    code AS transactionCode,
    invoiceId AS referenceTransactionId,
    returnDate AS transactionDate,
    branchId,
    receivedById AS employeeId,
    customerId,
    returnTotal AS total,
    returnFeeRatio,
    returnDiscount,
    totalPayment,
    returnFee,
    statusValue AS transactionStatus,
    saleChannelId,
    createdDate,
    modifiedDate,
FROM
    {{ ref('base_kiotViet__returns') }}
