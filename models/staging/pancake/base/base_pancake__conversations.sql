WITH source AS (
    {{ dbt_utils.deduplicate(relation = source('pancake', 'conversations'), partition_by = 'id', order_by = "_batched_at desc",) }}
)
SELECT
    source.id AS conversation_id,
    date_add(datetime(
        source.inserted_at
    ), interval 7 hour) AS inserted_at,
    source.type,
    customers.fb_id AS customer_fb_id,
    customers.id AS customer_id,
    source.last_sent_by.admin_id AS user_id,
    source.last_sent_by.admin_name AS user_name,
    source.last_sent_by.name AS page_name,
    source.message_count,
    source.post_id,
    source.page_id,
    date_add(source.updated_at, interval 7 hour) as updated_at,
    tags.id AS tag_id,
    tags.text AS tag_value,
    source.snippet,
    ads.ad_id, 
    source.tag_histories
FROM
    source
    left join unnest(tags) tags
    left join unnest(customers) customers
    left join unnest(ads) ads