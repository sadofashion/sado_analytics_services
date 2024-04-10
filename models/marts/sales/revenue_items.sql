{{ config(
    tags = ['incremental', 'fact','kiotviet','hourly'],
    materialized = 'incremental',
    partition_by ={ 'field': 'transaction_date',
    'data_type': 'timestamp',
    'granularity': 'day' },
    incremental_strategy = 'insert_overwrite',
    unique_key = ['transaction_id','product_id','price'],
    on_schema_change = 'sync_all_columns'
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
    invoices.product_id,
    invoices.product_code,
    invoices.price,
    invoices.transaction_type,
    invoices.modified_date,
    sum(invoices.quantity) quantity,
    avg(COALESCE(
        invoices.discount_ratio,
        safe_divide(
            invoices.discount * 100,
            invoices.price
        ),
        0
    )) discount_ratio,
    avg(invoices.discount) discount,
    sum(invoices.subTotal) subTotal,
FROM
    {{ ref('stg_kiotviet__invoicedetails') }}
    invoices
WHERE
    invoices.transaction_status = 'Hoàn thành'
    AND invoices.quantity <> 0
{% if is_incremental() %}
AND date(invoices.transaction_date) >= date_add(DATE(_dbt_max_partition), interval -1 day)
{% endif %}
{{dbt_utils.group_by(13)}}

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
    returns.product_id,
    returns.product_code,
    returns.price,
    returns.transaction_type,
    returns.modified_date,
    sum(returns.quantity) quantity, 
    CAST(
        NULL AS float64
    ) AS discount_ratio,
    CAST(
        NULL AS float64
    ) AS discount,
    sum(returns.price*returns.quantity) subtotal,
FROM
    {{ ref('stg_kiotviet__returndetails') }}
    returns
WHERE
    returns.transaction_status = 'Đã trả'

{% if is_incremental() %}
AND date(returns.transaction_date) >= date_add(DATE(_dbt_max_partition), interval -1 day)
{% endif %}
{{dbt_utils.group_by(13)}}
