SELECT
    returns.id AS transaction_id,
    returns.code AS transaction_code,
    returns.invoiceId AS reference_transaction_id,
    returns.returnDate AS transaction_date,
    returns.statusValue AS transaction_status,
    returns.branchId as branch_id,
    returns.receivedById AS employee_id,
    returns.customerId as customer_id,
    payments.id AS payment_id,
    payments.code AS payment_code,
    payments.statusValue AS payment_status,
    payments.transDate as payment_date,
    payments.amount AS payment_amount,
    payments.method AS payment_method,
    payments.accountId AS bankaccount_id,
FROM
    {{ ref('base_kiotViet__returns') }}
    returns,
    unnest(payments) payments
