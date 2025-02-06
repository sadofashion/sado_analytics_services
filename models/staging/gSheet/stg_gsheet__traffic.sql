with source as (
    select
        safe.parse_date('%d/%m/%Y',json_value(data, '$.date')) as date,
        json_value(data, '$.branch_name') as branch_name,
        json_value(data, '$.employee_name') as employee_name,
        safe_cast(replace(json_value(data,'$.traffic'),',','.') as float64) as traffic,
        safe_cast(replace(json_value(data,'$.traffic'),',','.') as float64) as working_hour,
        _batched_at 
    from {{ source('gSheet', 'traffic') }}
    where nullif(json_value(data, '$.traffic'),'undefined') is not null
        and nullif(json_value(data, '$.date'),'') is not null
        )

{{dbt_utils.deduplicate(
    relation='source',
    partition_by='date, branch_name, employee_name',
    order_by='_batched_at desc'
)}}