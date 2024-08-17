{%set statuses = {
    "SHIPPED": ['READY_TO_SHIP','PROCESSED','RETRY_SHIP','SHIPPED'],
    "CANCELLED": ['CANCELLED','IN_CANCEL'],
    "TO_RETURN":["TO_RETURN"],
    "COMPLETED":["COMPLETED"],
    "UNPAID":["UNPAID"],
    "TO_CONFIRM_RECEIVE":["TO_CONFIRM_RECEIVE"],
}%}


with source as (
    {{
        dbt_utils.deduplicate(
            relation = source('shopee', 'order_list'), 
            partition_by = 'json_value(data,"$.order_sn")', 
            order_by = '_batched_at desc'
            )
            }}
)

select 
json_value(o.data,'$.cancel_by') as cancel_by,
json_value(o.data,'$.cancel_reason') as cancel_reason,
date_add(timestamp_seconds(safe_cast(json_value(o.data,'$.create_time') as int64)),interval 7 hour) as create_time,
date_add(timestamp_seconds(safe_cast(json_value(o.data,'$.update_time') as int64)), interval 7 hour) as update_time,
safe_cast(json_value(o.data,'$.total_amount') as float64) as total_amount,
json_value(o.data,'$.order_sn') as order_sn,
case  {% for k,v in statuses.items()-%}
    when json_value(o.data,'$.order_status') in ("{{v|join('","')}}") then "{{k}}"
{% endfor -%} end as order_status,
json_value(o.data,'$.payment_method') as payment_method,
json_extract_array(o.data,'$.item_list') item_list,
json_extract_array(o.data,'$.package_list') package_list,
from source o