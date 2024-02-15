{{
    config(
        materialized='incremental',
        unique_key='engagement_id',
        partition_by ={ 'field': 'start_time',
        'data_type': 'timestamp',
        'granularity': 'day' },
        incremental_strategy = 'insert_overwrite',
        on_schema_change = 'sync_all_columns',
        tags=['caresoft','fact','incremental','inactive']
    )
}}

select
ticket_id,
customer_id,
safe_cast(service_id as int64) service_id,
start_time,
end_time,
safe_cast(agent_id as int64) agent_id,
call_id as engagement_id,
call_status as engagement_status,
case call_type 
    when 0 then 'call in'
    when 1 then 'call out'
    when 3 then 'IVR call' 
end as engagement_type,
unix_seconds(parse_timestamp('%H:%M:%S',talk_time)) as engagement_duration,
unix_seconds(parse_timestamp('%H:%M:%S',wait_time)) as wait_duration,
missed_reason,
'hotline' as engagement_channel,
from 
{{ref("stg_caresoft__calls")}} calls
{% if is_incremental() %}
                WHERE
                      date(start_time) >= date(_dbt_max_partition)

                   OR date(start_time) >= date_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
{% endif %}

union all

select
ticket_id,
customer_id,
service_id,
timestamp(start_time) start_time,
timestamp(end_time) end_time,
safe_cast(agents.id as int64) agent_id,
conversation_id as engagement_id,
case chat_status 
    when 'LBL_CHAT_STATUS_MISS' then 'miss'
    when 'LBL_CHAT_STATUS_MEET' then 'metAgent'
 end as engagement_status,
conversation_type as engagement_type,
chat_duration as engagement_duration,
3600 * cast(regexp_extract(wait_time, r'^\d*') as int64) + unix_seconds(parse_timestamp('%H:%M:%S',regexp_replace('44:01:35', r'^\d*', '00'))) as wait_duration,
cast(null as string) missed_reason,
conversation_type as engagement_channel,

from 
{{ref("stg_caresoft__chats")}} chats
left join {{ref("stg_caresoft__agents")}} agents on agents.email = chats.agent_email

{% if is_incremental() %}
                WHERE
                      date(start_time) >= date(_dbt_max_partition)

                   OR date(start_time) >= date_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
{% endif %}