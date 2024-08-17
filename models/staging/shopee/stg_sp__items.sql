with source as (
    {{
        dbt_utils.deduplicate(
            relation = source('shopee', 'item_list'), 
            partition_by = 'json_value(data,"$.item_id")', 
            order_by = '_batched_at desc'
            )
            }}
)

select 
json_value(o.data,'$.item_id') as item_id,
json_value(o.data,'$.item_name') as item_name,
json_value(o.data,'$.item_sku') as item_sku,
json_value(o.data,'$.item_status') as item_status,
json_value(o.data,'$.category_id') as category_id,
json_value(o.data,'$.brand.brand_id') as brand_id,
json_value(o.data,'$.brand.original_brand_name') as brand_name,

from source o