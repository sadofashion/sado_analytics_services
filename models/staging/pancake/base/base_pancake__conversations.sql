{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
        "field": "inserted_at",
        "data_type": "datetime",
        "granularity": "day"
    },
    on_schema_change = 'sync_all_columns',
    tags = ['pancake']
    )
}}


WITH source AS (
    select * from 
    {{source('pancake', 'conversations')}}
    where 1=1
    {% if is_incremental() %}
    and date(date_add(datetime(inserted_at), INTERVAL 7 HOUR)) in (
        select 
        date(date_add(datetime(inserted_at), INTERVAL 7 HOUR)) 
        from {{source('pancake', 'conversations')}}
      where parse_date('%Y%m%d', _TABLE_SUFFIX) >= date_sub(CURRENT_DATE(), interval 3 day)
    )
    {% endif %}
),

deduplicate as (
    {{ dbt_utils.deduplicate(relation = 'source', partition_by = 'id', order_by = "_batched_at desc",) }}
)

SELECT
    source.id AS conversation_id,
    date_add(datetime(source.inserted_at), INTERVAL 7 HOUR) AS inserted_at,
    source.type,
    customers.fb_id AS customer_fb_id,
    customers.id AS customer_id,
    source.last_sent_by.admin_id AS user_id,
    source.last_sent_by.admin_name AS user_name,
    source.last_sent_by.name AS page_name,
    source.message_count,
    source.post_id,
    source.page_id,
    date_add(
        source.updated_at,
        INTERVAL 7 HOUR
    ) AS updated_at,
    tags.id AS tag_id,
    tags.text AS tag_value,
    source.snippet,
    ads.ad_id,
    source.tag_histories
FROM
    deduplicate as source
    LEFT JOIN unnest(tags) tags
    LEFT JOIN unnest(customers) customers
    LEFT JOIN unnest(ads) ads
WHERE
    (
        source.last_sent_by.name
    ) LIKE '%5S%'
