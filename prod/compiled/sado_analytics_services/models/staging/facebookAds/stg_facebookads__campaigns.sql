

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
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`Facebook`.`p_AdsInsights__*`
                LIMIT
                    1000
            )
        

        
)
SELECT
    *,
    regexp_extract (
        campaign_name,
        r"^(?:.*?_){4}(.*?)_(?:.*?)$"
    ) AS big_campaign,

    regexp_extract (
        campaign_name,
        r"^(?:.*?_){4}(.*?_.*?)_(?:.*?)$"
    ) AS content_group,

    regexp_extract (
        campaign_name,
        r"^(?:.*?_){1}(.*?)_(?:.*?)$"
    ) AS pic,

    regexp_extract (
        campaign_name,
        r"^(?:.*?_){6}(.*?)_(?:.*?)$"
    ) AS promoted_productline,

    regexp_extract (
        campaign_name,
        r"^(.*?)_"
    ) AS page,

    regexp_extract (
        campaign_name,
        r"(?:.*?_){7}(.*?)_(?:.*?)"
    ) AS media_type,

    regexp_extract (
        campaign_name,
        r"^(?:.*?_){2}(.*?)_(?:.*?)$"
    ) AS funnel,

    regexp_extract (
        campaign_name,
        r"^(?:.*?_){3}(.*?)_(?:.*?)$"
    ) AS ad_type,

FROM
    source