with base_orders as (
    {{
        dbt_utils.deduplicate(
            relation = source('ab_web', 'orders'),
            partition_by = 'id',
            order_by = '_airbyte_extracted_at desc'
        )
    }}

)

{# base_order_shippings as (
    {{
        dbt_utils.deduplicate(
            relation = source('web', 'order_shipings'),
            partition_by = 'id',
            order_by = '_batched_at desc'
        )
    }}
)


select 
-- orders
json_value(o.data,'$.id') as order_id,
json_value(o.data,'$.code') as order_code,
json_value(o.data,'$.token') as token,
timestamp(json_value(o.data,'$.created_at')) as created_at,
timestamp(json_value(o.data,'$.updated_at')) as updated_at,
safe_cast(json_value(o.data,'$.total_price') as float64) as total_price,
safe_cast(json_value(o.data,'$.total_ship') as float64) as total_ship,
safe_cast(json_value(o.data,'$.total') as float64) as total,

--order shipings
nullif(json_value(os.data,'$.phone'),"") as customer_phone,
json_value(os.data,'$.name') as customer_name,
nullif(json_value(os.data,'$.email'),"0") as customer_email,
{{dbt_utils.generate_surrogate_key(["json_value(os.data,'$.province_id')","json_value(os.data,'$.district_id')","json_value(os.data,'$.ward_id')"])}} as location_id,

from base_orders o
left join base_order_shippings os on json_value(o.data,'$.id') = json_value(os.data,'$.order_id')  #}

select id, code, 
total_sub as total_discount,
total_price as original_price,
total_ship as shipping_fee,
total,
created_at as transaction_date,
invoice_id,
case status when 1 then "Confirmed" when 2 then "Synced" when 3 then "Cancelled" else "Unknown" end as status,
from base_orders
