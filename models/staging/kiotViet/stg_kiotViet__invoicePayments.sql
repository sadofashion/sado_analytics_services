SELECT
    invoices.id AS transactionId,
    invoices.code AS transactionCode,
    invoices.orderCode as referenceTransactionId,
    invoices.purchaseDate AS transactionDate,
    invoices.statusValue AS transactionStatus,
    payments.id AS paymentId,
    payments.code AS paymentCode,
    payments.statusValue AS paymentStatus,
    payments.transDate as paymentDate,
    payments.amount AS paymentAmount,
    payments.method AS paymentMethod,
    payments.accountId AS bankAccountId,
FROM
    {{ ref('base_kiotViet__invoices') }}
    invoices,
    unnest(payments) payments
