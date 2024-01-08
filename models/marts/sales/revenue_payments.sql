{{ config(
    materialized = 'incremental',
    partition_by ={ 'field': 'transaction_date',
    'data_type': 'timestamp',
    'granularity': 'day' },
    incremental_strategy = 'insert_overwrite',
    unique_key = ['transaction_date','transaction_id'],
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'daily','fact','kiotviet']
) }}

SELECT
    invoices.transaction_id,
    invoices.transaction_code,
    CAST(
        NULL AS int64
    ) AS reference_transaction_id,
    invoices.transaction_date,
    invoices.transaction_status,
    invoices.branch_id,
    invoices.employee_id,
    invoices.customer_id,
    invoices.payment_id,
    invoices.payment_code,
    invoices.payment_status,
    invoices.payment_date,
    invoices.payment_amount,
    invoices.payment_method,
    invoices.bankaccount_id,
    invoices.transaction_type,
FROM
    {{ ref('stg_kiotviet__invoicepayments') }}
    invoices
WHERE
    invoices.transaction_status = 'Hoàn thành'
UNION ALL
SELECT
    returns.transaction_id,
    returns.transaction_code,
    returns.reference_transaction_id,
    returns.transaction_date,
    returns.transaction_status,
    returns.branch_id,
    returns.employee_id,
    returns.customer_id,
    returns.payment_id,
    returns.payment_code,
    returns.payment_status,
    returns.payment_date,
    returns.payment_amount,
    returns.payment_method,
    returns.bankaccount_id,
    returns.transaction_type
FROM
    {{ ref('stg_kiotviet__returnpayments') }}
    returns
WHERE
    returns.transaction_status = 'Đã trả'
