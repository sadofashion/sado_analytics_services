SELECT
    orders.* except(products),
    products.product_id,
    products.product_code,
    products.price,
    products.quantity,
    products.item_discount,
FROM
    {{ ref('stg_nhanhvn__ordersdetails') }}
    orders,
    unnest(products) products