{{ config(
    tags = ['incremental', 'fact','nhanhvn','hourly'],
    materialized = 'incremental',
    partition_by ={ 'field': 'transaction_date',
    'data_type': 'date',
    'granularity': 'day' },
    incremental_strategy = 'insert_overwrite',
    unique_key = ['product_code','price'],
    on_schema_change = 'sync_all_columns'
) }}

SELECT
        delivery_date AS transaction_date,
        cast(order_id as string) AS transaction_code,
        traffic_source_id AS branch_id,
        coalesce(cic.kiotviet_customer_id, s.customer_id) customer_id,
        product_id,
        product_code,
        price AS price,
        CASE
            order_type
            WHEN 'Giao hàng tận nhà' THEN 'invoice'
            WHEN 'Khách trả lại hàng' THEN 'return'
        END AS transaction_type,
        'nhanhvn' AS source,
        sum(quantity) AS quantity,
        safe_divide(sum(item_discount),sum(price*quantity)) as discount_ratio,
        sum(item_discount) as discount,
        {# sum(case when item_gross_amount= 0 then quantity end) as gift_qty, #}
        sum(item_discount) as order_discount,
        sum(item_gross_amount) as subtotal,
        {# count(distinct order_id) as num_transaction_lines, #}
        {# count(distinct product_code) as num_products, #}
    FROM
        {{ ref("orders_items") }} s 
        left join {{ ref("int_customer_id_converter") }} cic on s.customer_id = cic.nhanhvn_customer_id
        where s.order_status IN ("Thành công")
        {% if is_incremental() %}
          and delivery_date in (
            select distinct delivery_date from {{ ref("orders_items") }} 
            where date(last_sync) >= date_add(current_date, interval -1 day)
            )
        {% endif %}
    {{dbt_utils.group_by(8)}}