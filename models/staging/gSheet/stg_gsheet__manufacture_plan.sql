with source as (
    {{
        dbt_utils.deduplicate(
        relation = source(
            'gSheet',
            'manufacture_plan'
        ),
        partition_by = "json_value(data,'$.product_code'),json_value(data,'$.vendor'),json_value(data,'$.month'),json_value(data,'$.year')",
        order_by = "_batched_at desc",
    )
    }}

)


select 
    json_value(data,'$.product_code') as product_code,
    json_value(data,'$.original_code') as original_code,
    nullif(json_value(data,'$.vendor'),'') as vendor,
    coalesce(
        SAFE.parse_date('%a %b %d %Y  00:00:00 GMT+0700 (Indochina Time)',json_value(data,"$.expected_deliver_date")),
        SAFE.parse_date('%d/%m %Y',regexp_extract(json_value(data,"$.expected_deliver_date"), r'(\d{2}/\d{2})' )||" "|| json_value(data,'$.year') )
     ) as expected_deliver_date,
    safe_cast(json_value(data,"$.expected_volumn") as float64) expected_deliver_amount,
    nullif(json_value(data,'$.po_code'),'') as po_code,
    json_value(data,'$.year')||json_value(data,'$.month') as plan_month,
from source
