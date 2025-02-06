WITH source AS (
    {{ dbt_utils.deduplicate(
        relation = source(
            'facebookAds',
            'p_CreativesInsights__*'
        ),
        partition_by = 'account_id,
            ad_id,
            id',
        order_by = "_batched_at desc",
    ) }}
)
SELECT
    account_id,
    ad_id,
    body,
    image_url,
    thumbnail_url,
    call_to_action_type,
    title,
    name,
FROM
    source
