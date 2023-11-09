{{ config(
    tags = ['ggads','fact','view']
) }}

WITH 
campaign_stats AS (
    SELECT
        campaign_id,
        date,
        slot,
        SUM(cost) AS ads_cost,
        SUM(impressions) AS ads_impressions,
        SUM(clicks) AS ads_clicks,
        SUM(conversions) AS ads_conversions,
        SUM(interactions) AS ads_interactions,
    FROM
        {{ ref('stg_googleads__campaign_stats') }}
    GROUP BY
        1,
        2,
        3
),
campaign_conversion_stats AS (
    SELECT
        campaign_id,
        date,
        slot,
        SUM(
            CASE
                WHEN ((conversion_name) IN ('website_purchase')) THEN (conversions)
                ELSE NULL
            END
        ) AS ads_purchase,
        SUM(
            CASE
                WHEN ((conversion_name) IN ('website_purchase')) THEN (conversions_value)
                ELSE NULL
            END
        ) AS ads_purchase_value,
        SUM(
            CASE
                WHEN ((conversion_name) IN ('add_to_cart')) THEN (conversions_value)
                ELSE NULL
            END
        ) AS ads_atc,
        SUM(
            CASE
                WHEN ((conversion_name) IN ('website_call', 'website_message')) THEN (conversions_value)
                ELSE NULL
            END
        ) AS ads_contacts,
    FROM
        {{ ref('stg_googleads__campaign_conversion_stats') }}
    GROUP BY
        1,
        2,
        3
)
SELECT
    cs.*,
    ccs.ads_purchase,
    ccs.ads_purchase_value,
    ccs.ads_contacts,
    ccs.ads_atc,
FROM
    campaign_stats cs
    LEFT JOIN campaign_conversion_stats ccs
    ON cs.campaign_id = ccs.campaign_id
    AND cs.date = ccs.date
    AND cs.slot = ccs.slot
