{{
  config(
    materialized = 'incremental',
    partition_by ={ 'field': 'last_purchase_date',
    'data_type': 'date',
    'granularity': 'day' },
    incremental_strategy = 'merge',
    unique_key = ['customer_id'],
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'daily','fact','kiotviet','nhanhvn']
    )
}}

with updated_customers as (
    select distinct customer_id
    from {{ ref("fct__transactions") }}
    {% if is_incremental() %}
      where transaction_date >= date(_dbt_max_partition)
    {% endif %}
)

SELECT
    u.customer_id,
    COUNT(
        DISTINCT CASE
            WHEN transaction_type = 'invoice' THEN t.transaction_source_id
        END
    ) AS num_transactions,
    SUM(
        t.total
    ) AS total_purchased_goods_value,
    SUM(
        t.total_payment
    ) AS total_monetary_value,
    MAX(
        t.transaction_date
    ) AS last_purchase_date,
    MIN(
        t.transaction_date
    ) AS first_purchase_date,
    COUNT(
        CASE
            WHEN t.source = 'nhanhvn' THEN t.transaction_id
        END
    ) > 1 AS has_online_purchase,
FROM
    updated_customers u
    inner join {{ ref("fct__transactions") }} t on u.customer_id = t.customer_id
    {{ dbt_utils.group_by(1) }}
