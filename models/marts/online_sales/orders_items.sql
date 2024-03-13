{{ config(
    tags = ['incremental', 'fact','nhanhvn'],
    materialized = 'incremental',
    partition_by ={ 'field': 'created_date',
    'data_type': 'datetime',
    'granularity': 'day' },
    incremental_strategy = 'merge',
    unique_key = ['order_id','product_id','price'],
    on_schema_change = 'sync_all_columns'
) }}

{%set order_statuses = ['Mới',
        'Chờ xác nhận',
        'Đang xác nhận',
        'Đã xác nhận',
        'Đổi kho hàng',
        'Đang đóng gói',
        'Đã đóng gói',
        'Chờ thu gom',
        'Đang chuyển',
        'Thành công']%}


SELECT
    orders.order_id,
    orders.sales_channel_id,
    orders.depot_name,
    orders.customer_id,
    COALESCE(
        orders.created_by_id,
        orders.sale_id
    ) AS created_by_id,
    orders.traffic_source_name,
    orders.order_type,
    orders.order_status,
    orders.created_date,
    orders.product_id,
    carriers.service_name,
    orders.delivery_date,
    orders.price,
    orders.quantity,
    orders.ship_address,
    orders.item_discount,
    orders.order_discount /(COUNT(product_id) over w1) order_discount,
    orders.money_used_points /(COUNT(product_id) over w1) AS money_used_points,
    date_diff(orders.delivery_date, DATE(orders.created_date), DAY) /(COUNT(product_id) over w1) AS fulfillment_time,
    ((orders.price - orders.item_discount) * orders.quantity) AS item_gross_amount,
    (
        orders.ship_fee + orders.cod_fee
    ) /(COUNT(product_id) over w1) AS delivery_fee,
    (
        orders.receivables + orders.money_transfer + orders.money_deposit + orders.customer_ship_fee
    ) /(COUNT(product_id) over w1) AS sub_total,
    (COUNT(product_id) over w1) AS order_total_lines,
FROM
    {{ ref('int_nhanhvn__expand_order_details') }}
    orders
    LEFT JOIN {{ ref('stg_nhanhvn__carriers') }}
    carriers
    ON orders.carrier_id = carriers.carrier_id
    AND orders.service_id = carriers.service_id
WHERE
    orders.order_status IN (
        {%for status in order_statuses%} '{{status}}' {{',' if not loop.last}}{%endfor%}
    ) window w1 AS (
        PARTITION BY orders.order_id
    )