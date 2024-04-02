{{ config(
    materialized = 'incremental',
    partition_by ={ 'field': 'transaction_date',
    'data_type': 'date',
    'granularity': 'day' },
    incremental_strategy = 'insert_overwrite',
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'hourly','fact','kiotviet','nhanhvn']
) }}

WITH customer_id_converter AS (

    SELECT
        kiotviet_customer_id,
        nhanhvn_customer_id
    FROM
        {{ ref("fct__customers") }}
    WHERE
        kiotviet_customer_id IS NOT NULL
        AND nhanhvn_customer_id IS NOT NULL
),

kiotviet_details AS (

    SELECT
        {# product_id AS kiotviet_product_id, #}
        branch_id AS branch_id,
        customer_id AS customer_id,
        product_code,

        date(transaction_date) as transaction_date,
        transaction_type,
        'kiotviet' AS transaction_source,

        price AS price,
        sum(quantity) AS quantity,
        sum(discount) AS discount,
        sum(subtotal) subtotal,
        count(distinct transaction_id) as num_transactions,
    FROM
        {{ ref('revenue_items') }}
        where 1=1
        {% if is_incremental() %}
          and date(transaction_date) >= date(_dbt_max_partition)
        {% endif %}
    {{dbt_utils.group_by(7)}}
),
nhanhvn_details AS (
    SELECT
        {# product_id AS nhanhvn_product_id, #}
        traffic_source_id AS branch_id,
        coalesce(customer_id_converter.kiotviet_customer_id, s.customer_id) customer_id,
        product_code,
        delivery_date AS transaction_date,
        CASE
            order_type
            WHEN 'Giao hàng tận nhà' THEN 'invoice'
            WHEN 'Khách trả lại hàng' THEN 'return'
        END AS transaction_type,
        'nhanhvn' AS transaction_source,

        price AS price,
        sum(quantity) AS quantity,
        sum(item_discount) as discount,
        sum(sub_total) as subtotal,
        count(distinct order_id) as num_transactions,
    FROM
        {{ ref("orders_items") }} s 
        left join customer_id_converter on s.customer_id = customer_id_converter.nhanhvn_customer_id
        where s.order_status IN ("Thành công")
        {% if is_incremental() %}
          and delivery_date >= date(_dbt_max_partition)
        {% endif %}
    {{dbt_utils.group_by(7)}}
)

select *,
FROM
    kiotviet_details
UNION ALL
SELECT
    *,
FROM
    nhanhvn_details