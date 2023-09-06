SELECT
    invoices.id AS transaction_id,
    invoices.code AS transaction_code,
    invoices.orderCode as reference_transaction_id,
    invoices.purchaseDate AS transaction_date,
    invoices.statusValue AS transaction_status,
    payments.id AS payment_id,
    payments.code AS payment_code,
    payments.statusValue AS payment_status,
    payments.transDate as payment_date,
    payments.amount AS payment_amount,
    payments.method AS payment_method,
    payments.accountId AS bankaccount_id,
FROM
    {{ ref('base_kiotViet__invoices') }}
    invoices,
    unnest(payments) payments
