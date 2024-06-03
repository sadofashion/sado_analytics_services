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

WITH customer_id_converter AS (

    SELECT
        kiotviet_customer_id,
        nhanhvn_customer_id
    FROM
        {{ ref("fct__customers") }}
    WHERE
        kiotviet_customer_id IS NOT NULL
        AND nhanhvn_customer_id IS NOT NULL
)

SELECT
        order_id AS transaction_id,
        shop_order_id AS transaction_code,
        delivery_date AS transaction_date,
        return_from_order_id AS reference_transaction_id,
        traffic_source_id AS branch_id,
        COALESCE(
            customer_id_converter.kiotviet_customer_id,
            customer_id
        ) AS customer_id,
        created_by_id AS employee_id,
        receivables AS total,
        (
            receivables + money_transfer - customer_ship_fee
        ) AS total_payment,
        order_discount AS discount,
        safe_divide(
            order_discount,
            order_discount + receivables
        ) AS discount_ratio,
        return_fee,
        CASE
            order_type
            WHEN 'Giao hàng tận nhà' THEN 'invoice'
            WHEN 'Khách trả lại hàng' THEN 'return'
        END AS transaction_type,
        COALESCE(
            delivery_date,
            send_carrier_date,
            DATE(created_date)
        ) AS modified_date,
        'nhanhvn' AS source,
    FROM
        {{ ref("stg_nhanhvn__ordersdetails") }} orders
        LEFT JOIN customer_id_converter
        ON orders.customer_id = customer_id_converter.nhanhvn_customer_id
    WHERE
        1 = 1
{% if is_incremental() %}
and delivery_date in (
    select 
    COALESCE(
            delivery_date,
            send_carrier_date,
            DATE(created_date)
        )
    from {{ ref('stg_nhanhvn__ordersdetails') }}
    where date(last_sync) >= date_add(CURRENT_DATE, INTERVAL -1 DAY)
    and delivery_date is not null
    AND order_status IN ("Thành công")
    AND order_type IS NOT NULL
)
{% endif %}
AND order_status IN ("Thành công")
AND order_type IS NOT NULL