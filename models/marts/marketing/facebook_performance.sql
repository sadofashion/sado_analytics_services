SELECT
    campaigns.account_name,
    adsinsights.date_start,
    campaigns.campaign_name,
    campaigns.big_campaign,
    campaigns.pic,
    campaigns.content_group,
    campaigns.page,
    campaigns.promoted_productline,
    campaigns.media_type,
    sum(adsinsights.impressions) impressions,
    sum(adsinsights.spend) spend,
    sum(adsinsights.clicks) clicks,
    sum(adsinsights.reach) reach,
    sum(adsinsights.link_click) link_click,
    sum(adsinsights.post_engagement) post_engagement,
    sum(adsinsights.offline_conversion_purchase) offline_conversion_purchase,
    sum(adsinsights.offline_conversion_purchase_value) offline_conversion_purchase_value,
    sum(adsinsights.pixel_purchase) pixel_purchase,
    sum(adsinsights.pixel_purchase_value) pixel_purchase_value,
    sum(adsinsights.meta_purchase) meta_purchase,
    sum(adsinsights.meta_purchase_value) meta_purchase_value,
    sum(adsinsights.messaging_conversation_started_7d) messaging_conversation_started_7d
FROM
    {{ ref('stg_facebookads__adsinsights') }} adsinsights
    left join {{ref('stg_facebookads__campaigns')}} campaigns on adsinsights.campaign_id = campaigns.campaign_id
    group by 1,2,3,4,5,6,7,8,9
