SELECT
    returns.id AS transaction_id,
    returns.code AS transactionCode,
    returns.invoiceId AS referencetransaction_id,
    returns.returnDate AS transaction_date,
    returns.statusValue AS transactionStatus,
    payments.id AS paymentId,
    payments.code AS paymentCode,
    payments.statusValue AS paymentStatus,
    payments.transDate as paymentDate,
    payments.amount AS paymentAmount,
    payments.method AS paymentMethod,
    payments.accountId AS bankAccountId,
FROM
    {{ ref('base_kiotViet__returns') }}
    returns,
    unnest(payments) payments
