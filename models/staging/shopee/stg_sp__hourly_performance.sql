{{
  config(
    tags=['fact', 'shopee'],
    )
}}

WITH source AS (
    {{ dbt_utils.deduplicate(
        relation = source('shopee','ad_hourly_performance'),
        partition_by = 'json_value(data, "$.date"), json_value(data, "$.hour")',
        order_by = '_batched_at desc'
    ) }}
)
SELECT
    parse_date('%d-%m-%Y',json_value(s.data,'$.date')) as date,
    safe_cast(json_value(s.data,'$.impression') as float64) as impression,
    safe_cast(json_value(s.data,'$.clicks') as float64) as clicks,
    safe_cast(json_value(s.data,'$.expense') as float64) as expense,
    safe_cast(json_value(s.data,'$.broad_order') as float64) as broad_order,
    safe_cast(json_value(s.data,'$.broad_gmv') as float64) as broad_gmv,
    safe_cast(json_value(s.data,'$.broad_item_sold') as float64) as broad_item_sold,
    safe_cast(json_value(s.data,'$.broad_roas') as float64) as broad_roas,
    safe_cast(json_value(s.data,'$.direct_order') as float64) as direct_order,
    safe_cast(json_value(s.data,'$.direct_gmv') as float64) as direct_gmv,
    safe_cast(json_value(s.data,'$.direct_item_sold') as float64) as direct_item_sold,
    safe_cast(json_value(s.data,'$.direct_roas') as float64) as direct_roas,
    safe_cast(json_value(s.data,'$.broad_conversions') as float64) as broad_conversions,
    safe_cast(json_value(s.data,'$.direct_conversions') as float64) as direct_conversions,
    safe_cast(json_value(s.data,'$.cost_per_conversion') as float64) as cost_per_conversion,
    safe_cast(json_value(s.data, '$.hour') as int64) as hour,
FROM
    source s
