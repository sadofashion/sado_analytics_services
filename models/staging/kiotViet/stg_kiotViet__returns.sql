SELECT
    id AS transaction_id,
    code AS transactionCode,
    invoiceId AS referencetransaction_id,
    returnDate AS transaction_date,
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
