{# json_value(item_list,'$.add_on_deal') as add_on_deal,
json_value(item_list,'$.add_on_deal_id') as add_on_deal_id,
json_value(item_list,'$.item_id') as item_id,
json_value(item_list,'$.item_name') as item_name,
json_value(item_list,'$.item_sku') as item_sku,
json_value(item_list,'$.main_item') as main_item,
json_value(item_list,'$.model_discounted_price') as model_discounted_price,
json_value(item_list,'$.model_id') as model_id,
json_value(item_list,'$.model_name') as model_name,
json_value(item_list,'$.model_original_price') as model_original_price,
json_value(item_list,'$.model_quantity_purchased') as model_quantity_purchased,
json_value(item_list,'$.model_sku') as model_sku,
json_value(item_list,'$.order_item_id') as order_item_id,
json_value(item_list,'$.promotion_group_id') as promotion_group_id,
json_value(item_list,'$.promotion_id') as promotion_id,
json_value(item_list,'$.promotion_type') as promotion_type,
json_value(item_list,'$.weight') as weight, #}

with source as (
    {{
        dbt_utils.deduplicate(
            relation = source('shopee', 'order_list'), 
            partition_by = 'id', 
            order_by = '_batched_at desc'
            )
            }}
)

select 
json_value(o.data,'$.cancel_by') as cancel_by,
json_value(o.data,'$.cancel_reason') as cancel_reason,
timestamp_seconds(json_value(o.data,'$.create_time')) as create_time,
timestamp_seconds(json_value(o.data,'$.update_time')) as update_time,
json_value(o.data,'$.total_amount') as total_amount,
json_value(o.data,'$.order_sn') as order_sn,
json_value(o.data,'$.order_status') as order_status,
json_value(o.data,'$.payment_method') as payment_method,
json_extract_array(o.data,'$.item_list') item_list,
json_extract_array(o.data,'$.package_list') package_list,
from source o