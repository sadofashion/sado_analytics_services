SELECT
    account_id,
    date_start,
    region,
    sum(impressions) impressions,
    sum(spend) spend,
    sum(clicks) clicks,
    sum(reach) reach,
    sum(link_click) link_click,
    sum(post_engagement) post_engagement,
    sum(offline_conversion_purchase) offline_conversion_purchase,
    sum(offline_conversion_purchase_value) offline_conversion_purchase_value,
    sum(pixel_purchase) pixel_purchase,
    sum(pixel_purchase_value) pixel_purchase_value,
    sum(messaging_conversation_started_7d) messaging_conversation_started_7d
FROM
    {{ ref('stg_facebookads__regioninsights') }}
    group by 1,2,3
