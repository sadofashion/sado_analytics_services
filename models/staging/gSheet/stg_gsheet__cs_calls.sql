with source as (
      {{dbt_utils.deduplicate(
        relation= source('gSheet', 'cs_calls'),
        partition_by = "json_value(data,'$.customer_phone'),json_value(data,'$.call_month'),json_value(data,'$.branch_name')",
        order_by = "_batched_at"
        )
      }}
)

select
    json_value(data, '$.asm') as asm,
    json_value(data, '$.branch_name') as branch_name,
    parse_date('%Y-T%m',json_value(data, '$.call_month')) as call_month,
    json_value(data, '$.customer_dob') as customer_dob,
    json_value(data, '$.customer_gender') as customer_gender,
    json_value(data, '$.customer_name') as customer_name,
    json_value(data, '$.customer_phone') as customer_phone,
    json_value(data, '$.customer_total_purchasing_value') as customer_total_purchasing_value,
    json_value(data, '$.call_status') as call_status,
from source

