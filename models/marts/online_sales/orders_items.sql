{{
  config(
    tags=['table', 'fact','nhanhvn']
  )
}}

SELECT
    orders.order_id,
    orders.sale_channel,
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
    orders.price,
    orders.quantity,
    orders.ship_address,
    orders.item_discount,
    orders.order_discount/(count(product_id) over w1) order_discount,
    orders.money_used_points/(count(product_id) over w1) as money_used_points,
    date_diff(orders.delivery_date, DATE(orders.created_date), DAY)/(count(product_id) over w1) AS fulfillment_time,
    ((orders.price - orders.item_discount) * orders.quantity) AS item_gross_amount,
    (
        orders.ship_fee + orders.cod_fee
    )/(count(product_id) over w1) AS delivery_fee,
    (
        orders.receivables + orders.money_transfer + orders.money_deposit + orders.customer_ship_fee
    )/(count(product_id) over w1) AS sub_total,
    (count(product_id) over w1) as order_total_lines,
FROM
    {{ ref('stg_nhanhvn__ordersdetails') }}
    orders
    LEFT JOIN {{ref('stg_nhanhvn__carriers')}} carriers
    ON orders.carrier_id = carriers.carrier_id
    AND orders.service_id = carriers.service_id
WHERE
    orders.order_status IN (
        'Mới',
        'Chờ xác nhận',
        'Đang xác nhận',
        'Đã xác nhận',
        'Đổi kho hàng',
        'Đang đóng gói',
        'Đã đóng gói',
        'Chờ thu gom',
        'Đang chuyển',
        'Thành công'
    )
window w1 as (
    PARTITION BY orders.order_id
)