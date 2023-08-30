WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY account_id,
            date_start
            ORDER BY
                _batched_at DESC
        ) AS rn_,
    FROM
        {{ source(
            'facebookAds',
            'p_AccountInsights__*'
        ) }}
)
SELECT
    account_id,
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
