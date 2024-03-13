SELECT
    orders.* except(products),
    products.product_id,
    products.product_code,
    products.price,
    sum(products.quantity) quantity,
    sum(products.item_discount) item_discount,
FROM
    {{ ref('stg_nhanhvn__ordersdetails') }}
    orders,
    unnest(products) products
    {{dbt_utils.group_by(40)}}