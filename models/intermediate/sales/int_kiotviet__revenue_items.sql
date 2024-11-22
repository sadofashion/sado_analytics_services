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
    date(invoices.transaction_date) transaction_date,
    invoices.transaction_code,
    invoices.branch_id,
    invoices.customer_id,
    invoices.product_id,
    invoices.product_code,
    invoices.price,
    invoices.transaction_type,
    {# invoices.modified_date, #}
    'kiotviet' as source,
    sum(invoices.quantity) quantity,
    avg(COALESCE(
        invoices.discount_ratio,
        safe_divide(invoices.discount * 100,invoices.price),0
        )) discount_ratio,
    avg(invoices.discount) discount,
    sum(invoices.order_discount) order_discount,
    sum(invoices.subtotal) subTotal,
FROM
    {{ ref('stg_kiotviet__invoicedetails') }} invoices
WHERE
    invoices.transaction_status = 'Hoàn thành'
    AND invoices.quantity <> 0
{% if is_incremental() %}
    AND date(invoices.transaction_date) in (
        select
        distinct date(transaction_date) 
        from {{ ref('stg_kiotviet__invoicedetails') }}
        where date(coalesce(modified_date,transaction_date)) >= date_add(current_date, interval -1 day)
    )
    {# and date(coalesce(invoices.modified_date,invoices.transaction_date)) >= date_add(current_date, interval -3 day) #}
{% endif %}
{{dbt_utils.group_by(9)}}