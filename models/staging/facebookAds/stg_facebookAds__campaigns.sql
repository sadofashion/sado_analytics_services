WITH source AS (
    SELECT
        DISTINCT account_id,
        FIRST_VALUE (account_name) over (
            PARTITION BY account_id
            ORDER BY
                _batched_at DESC
        ) AS account_name,
        FIRST_VALUE (campaign_name) over (
            PARTITION BY campaign_id
            ORDER BY
                _batched_at DESC
        ) AS campaign_name,
        MIN(date_start) over (
            PARTITION BY campaign_id
        ) AS campaign_start_date,
        MAX(date_start) over (
            PARTITION BY campaign_id
        ) AS campaign_stop_date,
        campaign_id
    FROM
        {{ source(
            'facebookAds',
            'p_AdsInsights__*'
        ) }}
)
SELECT
    *,
    regexp_extract (
        campaign_name,
        r"^(?:.*?_){3}(.*?)_(?:.*?)$"
    ) AS bigCampaign,
    regexp_extract (
        campaign_name,
        r"^(?:.*?_){3}(.*?_.*?)_(?:.*?)$"
    ) AS contentGroup,
    regexp_extract (
        campaign_name,
        r"^(?:.*?_){1}(.*?)_(?:.*?)$"
    ) AS pic,
    regexp_extract (
        campaign_name,
        r"^(?:.*?_){5}(.*?)_(?:.*?)$"
    ) AS promotedProductline,
    regexp_extract (
        campaign_name,
        r"^(.*?)_"
    ) AS page,
    regexp_extract (
        campaign_name,
        r"(?:.*?_){6}(.*?)_(?:.*?)"
    ) AS mediaType
FROM
    source
