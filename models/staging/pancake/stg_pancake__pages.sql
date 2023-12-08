WITH source AS (
    {{ dbt_utils.deduplicate(relation = source('pancake', 'pages'), partition_by = 'id', order_by = "_batched_at desc",) }}
)
SELECT
    source.avatar_url,
    source.business,
    source.connected,
    source.id AS page_id,
    source.last_global_id_crawl,
    source.name AS page_name,
    source.need_fix_webhook,
    source.page_content_sync_group_id,
    source.role_in_page,
    source.shop_id,
    source.special_feature,
    source.timezone,
    source.username,
    source.status,
FROM
    source
