WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'facebookAds',
            'p_AudienceInsights__*'
        ) }}
    WHERE
        date_start < '2024-07-01'
),
deduplicate AS (
    {{ dbt_utils.deduplicate(
        relation = "source",
        partition_by = 'account_id, campaign_id, adset_id, ad_id, date_start, age, gender',
        order_by = "_batched_at desc",
    ) }}
)
SELECT
    account_id,
    campaign_id,
    adset_id,
    ad_id,
    age,
    gender,
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
