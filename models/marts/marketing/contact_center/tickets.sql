{{
    config(
        materialized='incremental',
        unique_key='ticket_id',
        partition_by ={ 'field': 'created_at',
        'data_type': 'timestamp',
        'granularity': 'day' },
        incremental_strategy = 'insert_overwrite',
        on_schema_change = 'sync_all_columns',
        tags=['caresoft','fact','incremental']
    )
}}

{% set label_mapping = {
"Tần suất tương tác":"engagement_freq",
"Chi tiết công việc":"service_details",
"Tiến trình tư vấn":"procedure",
"Chi tiết bán hàng":"sales_details",
"Nhân viên xử lý":"employee",
"Phân loại tương tác":"engagement_category",
}%}
{% set tags = ['ad_id','ad_name','utm_campaign','utm_source','utm_medium']%}

with unnested as (
 select
ticket_id,
assignee_id,
requester_id as contact_id,
campaign_id,
ticket_subject,
ticket_status,
created_at,
updated_at,
ticket_source,
cf.custom_field_label as custom_field_label,
cf.value_label as custom_field_value,
regexp_extract(tags.name,r'^([a-z\-_]+)[\:\-\.]\s?.*$') as tag_key,
regexp_extract(tags.name,r'^[a-z\-_]+[\:\-\.]\s?(.*)$') as tag_value,
from {{ref('stg_caresoft__tickets')}}
left join unnest(custom_fields) custom_fields
left join unnest(tags) tags
left join {{ref("stg_caresoft__ticket_custom_fields")}} cf on custom_fields.id = cf.custom_field_id and custom_fields.value = cf.value_id

{% if is_incremental() %}
                WHERE
                      created_at >= timestamp(_dbt_max_partition)

                   OR created_at >= timestamp_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
{% endif %}

),
pivot_custom_fields as (
    select * 
    from unnested
    pivot (
    any_value(custom_field_value) for custom_field_label in (
        {%for key, value in label_mapping.items()%}
            '{{key}}' as {{value}} {{ ',' if not loop.last}}
        {%endfor%}
    )
)
),

pivot_tags as (
    select * 
    from pivot_custom_fields
     pivot (
    any_value(tag_value) for tag_key in (
        {%for value in tags%}
            '{{value}}' {{ ',' if not loop.last}}
        {%endfor%}
    )
)
)

select * from pivot_tags