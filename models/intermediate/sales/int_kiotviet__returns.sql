{{ config(
    materialized = 'incremental',
    partition_by ={ 'field': 'transaction_date',
    'data_type': 'date',
    'granularity': 'day' },
    incremental_strategy = 'insert_overwrite',
    unique_key = ['transaction_source_id'],
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'hourly','fact','kiotviet','nhanhvn']
) }}

SELECT
    returns.transaction_id,
    returns.transaction_code,
    DATE(
        returns.transaction_date
    ) transaction_date,
    returns.reference_transaction_id,
    returns.branch_id,
    returns.customer_id,
    returns.employee_id,
    (
        - returns.total
    ) AS total,
    returns.total_payment,
    returns.return_discount,
    CAST(
        NULL AS float64
    ) discount_ratio,
    returns.return_fee,
    returns.transaction_type,
    DATE(
        COALESCE(
            returns.modified_date,
            returns.transaction_date
        )
    ) AS modified_date,
    'kiotviet' AS source,
FROM
    {{ ref('stg_kiotviet__returns') }}
    returns
WHERE
    returns.transaction_status = 'Đã trả'

{% if is_incremental() %}
{# and date(returns.modified_date) >= date_add(DATE(_dbt_max_partition), INTERVAL -1 DAY) #}
AND date(returns.transaction_date )in (
    select 
    distinct DATE(transaction_date) 
    from {{ ref('stg_kiotviet__returns') }} 
    where DATE(modified_date)  >= date_add(current_date, INTERVAL -1 DAY)
)
{% endif %}