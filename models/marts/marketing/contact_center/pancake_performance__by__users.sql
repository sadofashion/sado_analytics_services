{{ config(
    materialized = 'incremental',
    unique_key = ['user_id','hour'],
    partition_by ={ 
        'field': 'hour',
        'data_type': 'datetime',
        'granularity': 'day',
        },
    incremental_strategy = 'insert_overwrite',
    on_schema_change = 'sync_all_columns',
    tags = ['pancake','fact','incremental']
) }}

with user_stats as (
    select 
    user_id,
    hour,
    avg(average_response_time) average_response_time,
    sum(comment_count) comment_count,
    sum(inbox_count) inbox_count,
    sum(phone_number_count) phone_number_count,
    sum(private_reply_count) private_reply_count,
    sum(unique_comment_count) unique_comment_count,
    sum(unique_inbox_count) unique_inbox_count,
    from {{ref("stg_pancake__user_stats")}}
    {% if is_incremental() %}
    where
         date(hour) >= date_add(date(_dbt_max_partition), interval -3 day)
        {% endif %}
        group by 1,2
),
orders as (
    select 
    seller_id,
    date_trunc(inserted_at, hour) as hour,
    count(distinct order_id) num_order,
    sum(total_price) as total_payment,
    from {{ref("stg_pancake__orders")}}
    where
    seller_id is not null
    and status = 'Đã xác nhận'
    {% if is_incremental() %}
    
         and date(inserted_at) >= date_add(date(_dbt_max_partition), interval -3 day)
        {% endif %}
    group by 1,2
)

select 
us.* except(hour, user_id),
coalesce(us.user_id, o.seller_id) as user_id,
coalesce(us.hour, o.hour) as hour,
o.num_order,
o.total_payment
from user_stats us
full join orders o on us.user_id = o.seller_id and us.hour = o.hour