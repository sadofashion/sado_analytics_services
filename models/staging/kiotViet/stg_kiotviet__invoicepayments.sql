SELECT
    invoices.id AS transaction_id,
    invoices.code AS transaction_code,
    invoices.orderCode as reference_transaction_id,
    invoices.purchaseDate AS transaction_date,
    invoices.statusValue AS transaction_status,
    invoices.branchId as branch_id,
    invoices.soldById AS employee_id,
    invoices.customerId as customer_id,
    payments.id AS payment_id,
    payments.code AS payment_code,
    payments.statusValue AS payment_status,
    payments.transDate as payment_date,
    payments.amount AS payment_amount,
    payments.method AS payment_method,
    payments.accountId AS bankaccount_id,
    invoices.transaction_type,
FROM
    {{ ref('base_kiotViet__invoices') }}
    invoices,
    unnest(payments) payments
