
select
bills.bill_id,
bills.bill_date,
bills.created_date,
bills.bill_type,
bills.bill_mode,
bills.related_bill_id,
coalesce(bills.order_id, bills2.order_id) order_id,
bills.depot_id,
bills.customer_id,
coalesce(bills.created_by_id,bills.sale_id) created_by_id,
products.product_id,
products.discount,
products.price,
products.quantity,
products.vat,
from {{ref('stg_nhanhvn__bills')}} bills, unnest(products) products
left join {{ref('stg_nhanhvn__bills')}} bills2 on bills.related_bill_id = bills2.bill_id