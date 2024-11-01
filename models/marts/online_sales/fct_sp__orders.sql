{{
  config(
    tags=['fact', 'shopee','table'],
    materialized='table'
    )
}}

{% set promotion_types = {
    "product_promotion":"Ưu đãi sản phẩm", 
    "flash_sale":"Flash Sale", 
    "bundle_deal": "Ưu đãi combo", 
    "add_on_deal_main": "Ưu đãi mua kèm sản phẩm chính", 
    "add_on_deal_sub": "Ưu đãi mua kèm sản phẩm phụ",
}
%}

with extracted_values as (
    select 
o.order_sn as order_number,
o.order_status,
o.cancel_by,
o.cancel_reason,
date(o.create_time) as transaction_date,
o.payment_method,
json_value(item_list,'$.add_on_deal') as add_on_deal,
json_value(item_list,'$.add_on_deal_id') as add_on_deal_id,
json_value(item_list,'$.item_id') as item_id,
json_value(item_list,'$.item_name') as item_name,
json_value(item_list,'$.item_sku') as item_sku,
safe_cast(json_value(item_list,'$.main_item') as bool) as main_item,
json_value(item_list,'$.model_id') as model_id,
json_value(item_list,'$.model_name') as model_name,
safe_cast(json_value(item_list,'$.model_discounted_price') as float64) as model_discounted_price,
safe_cast(json_value(item_list,'$.model_original_price') as float64) as model_original_price,
safe_cast(json_value(item_list,'$.model_quantity_purchased') as int64) as model_quantity_purchased,
json_value(item_list,'$.model_sku') as model_sku,
COALESCE(regexp_extract(json_value(item_list,'$.model_sku'), r'[A-Z]{3}(2[1-5])'), "Cũ") AS year,
json_value(item_list,'$.order_item_id') as order_item_id,
json_value(item_list,'$.promotion_group_id') as promotion_group_id,
json_value(item_list,'$.promotion_id') as promotion_id,
nullif(json_value(item_list,'$.promotion_type'),"") as promotion_type,
safe_divide(o.total_amount, count(json_value(item_list,'$.model_sku')) over(partition by o.order_sn)) as line_amount,
from {{ ref("stg_sp__orders") }} o
left join unnest(item_list) as item_list
)
select 
    * except(promotion_type),
    regexp_extract(model_sku,r'-?(\w+)') as product_code,
    {# COALESCE(
        REGEXP_EXTRACT(
            regexp_extract(model_sku,r'-?(\w+)$'),
            r'(?:Y0|YY|00|YB)?([A-Z]{3}\d{5})'), 
            REGEXP_EXTRACT(model_sku,r'(?:Y0|YY|00|YB)?([A-Z]{3}\d{5})')
            ) as class_code, #}
    CASE
        WHEN year = "24" THEN regexp_extract(model_sku, r"^(?:[ZXY0]{0,3})([BC0][A-Z]{3}[0-9]{3,5})")
        else regexp_extract(model_sku, r"^(?:[ZXY0]{0,2})([BC0]?[A-Z]{3}[0-9]{3,5})") end
    AS class_code,
    case  {% for k,v in promotion_types.items() -%}
    when promotion_type = "{{k}}" then "{{v}}"
    {% endfor -%}
    end as promotion_type
from extracted_values