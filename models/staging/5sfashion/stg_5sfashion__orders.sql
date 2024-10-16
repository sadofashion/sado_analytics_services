with base_orders as (
    {{
        dbt_utils.deduplicate(
            relation = source('web', 'orders'),
            partition_by = 'id',
            order_by = '_batched_at desc'
        )
    }}

),

base_order_shippings as (
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
left join base_order_shippings os on json_value(o.data,'$.id') = json_value(os.data,'$.order_id') 