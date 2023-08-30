WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY account_id,
            campaign_id,
            adset_id,
            ad_id,
            date_start,
            region
            ORDER BY
                _batched_at DESC
        ) AS rn_,
    FROM
        {{ source(
            'facebookAds',
            'p_RegionInsights__*'
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
WHERE
    rn_ = 1
