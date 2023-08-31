SELECT
    invoices.id AS transaction_id,
    invoices.code AS transactionCode,
    invoices.orderCode as referencetransaction_id,
    invoices.purchaseDate AS transaction_date,
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
