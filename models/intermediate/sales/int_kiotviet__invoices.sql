{{ config(
    materialized = 'incremental',
    partition_by ={ 'field': 'transaction_date',
    'data_type': 'date',
    'granularity': 'day' },
    incremental_strategy = 'insert_overwrite',
    unique_key = ['transaction_source_id'],
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'hourly','fact','nhanhvn']
) }}

SELECT
        invoices.transaction_id,
        invoices.transaction_code,
        DATE(invoices.transaction_date) transaction_date,
        CAST(NULL AS int64) AS reference_transaction_id,
        invoices.branch_id,
        invoices.customer_id,
        invoices.employee_id,
        invoices.total,
        invoices.total_payment,
        invoices.discount,
        invoices.discount_ratio,
        CAST(NULL AS int64) AS return_fee,
        invoices.transaction_type,
        DATE(
            COALESCE(
                invoices.modified_date,
                invoices.transaction_date
            )
        ) AS modified_date,
        'kiotviet' AS source,
    FROM
        {{ ref('stg_kiotviet__invoices') }}
        invoices
    WHERE
        invoices.transaction_status = 'Hoàn thành'

{% if is_incremental() %}

AND date(invoices.transaction_date )in (
    select 
    distinct DATE(transaction_date) 
    from {{ ref('stg_kiotviet__invoices') }} 
    where DATE(modified_date)  >= date_add(current_date, INTERVAL -1 DAY)
)
{%endif%}