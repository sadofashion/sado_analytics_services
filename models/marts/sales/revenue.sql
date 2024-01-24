{{ config(
    materialized = 'incremental',
    partition_by ={ 'field': 'transaction_date',
    'data_type': 'timestamp',
    'granularity': 'day' },
    incremental_strategy = 'merge',
    unique_key = ['transaction_id'],
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'daily','fact','kiotviet']
) }}

SELECT
    invoices.transaction_id,
    invoices.transaction_code,
    invoices.transaction_date,
    CAST(
        NULL AS int64
    ) AS reference_transaction_id,
    invoices.branch_id,
    invoices.customer_id,
    invoices.employee_id,
    invoices.total,
    invoices.total_payment,
    invoices.discount,
    invoices.discount_ratio,
    CAST(
        NULL AS int64
    ) AS return_fee,
    invoices.transaction_type,
FROM
    {{ ref('stg_kiotviet__invoices') }}
    invoices
WHERE
    invoices.transaction_status = 'Hoàn thành'
    {% if is_incremental() %}
      and date(invoices.modified_date) >= date_add(date(_dbt_max_partition), interval -3 day)
    {% endif %}
UNION ALL
SELECT
    returns.transaction_id,
    returns.transaction_code,
    returns.transaction_date,
    returns.reference_transaction_id,
    returns.branch_id,
    returns.customer_id,
    returns.employee_id,- returns.total AS total,
    returns.total_payment,
    returns.return_discount,
    CAST(
        NULL AS float64
    ) discount_ratio,
    returns.return_fee,
    returns.transaction_type
FROM
    {{ ref('stg_kiotviet__returns') }}
    returns
WHERE
    returns.transaction_status = 'Đã trả'
    {% if is_incremental() %}
      and date(returns.modified_date) >= date_add(date(_dbt_max_partition), interval -3 day)
    {% endif %}