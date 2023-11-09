WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY account_id,
            ad_id,
            id
            ORDER BY
                _batched_at DESC
        ) AS rn_,
    FROM
        {{ source(
            'facebookAds',
            'p_CreativesInsights__*'
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
WHERE
    rn_ = 1
