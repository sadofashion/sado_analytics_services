WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'facebookAds',
            'p_RegionInsights__*'
        ) }}
    WHERE
        date_start < '2024-07-01'
),
deduplicate AS (
    {{ dbt_utils.deduplicate(
        relation = source(
            'facebookAds',
            'p_RegionInsights__*'
        ),
        partition_by = 'account_id, campaign_id, adset_id, ad_id, date_start, region',
        order_by = "_batched_at desc",
    ) }}
)
SELECT
    account_id,
    campaign_id,
    adset_id,
    ad_id,
    region,
    date_start,
    clicks,
    impressions,
    spend,
    reach,
    actions,
    action_values,
    cost_per_action_type,
    cost_per_unique_action_type,
FROM
    source
