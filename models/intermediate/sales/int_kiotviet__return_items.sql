{{ config(
    tags = ['incremental', 'fact','kiotviet','hourly'],
    materialized = 'incremental',
    partition_by ={ 'field': 'transaction_date',
    'data_type': 'date',
    'granularity': 'day' },
    incremental_strategy = 'insert_overwrite',
    unique_key = ['product_id','price'],
    on_schema_change = 'sync_all_columns'
) }}

SELECT
    date(returns.transaction_date) transaction_date,
    returns.branch_id,
    returns.customer_id,
    returns.product_id,
    returns.product_code,
    returns.price,
    returns.transaction_type,
    {# returns.modified_date, #}
    'kiotviet' as source,
    -sum(returns.quantity) quantity, 
    CAST(NULL AS float64) AS discount_ratio,
    CAST(NULL AS float64) AS discount,
    -sum(returns.price*returns.quantity) subtotal,
FROM
    {{ ref('stg_kiotviet__returndetails') }}
    returns
WHERE
    returns.transaction_status = 'Đã trả'
{% if is_incremental() %}
AND date(returns.transaction_date) in (
        select 
        date(transaction_date) 
        from {{ ref('stg_kiotviet__returndetails') }}
        where date(coalesce(modified_date,transaction_date)) >= date_add(current_date, interval -1 day)
    )
{% endif %}
{{dbt_utils.group_by(8)}}
