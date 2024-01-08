{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'date_start',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'insert_overwrite',
  unique_key = ['date_start','campaign_id'],
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'daily','fact']
) }}

WITH facebook_performance AS (

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
    campaigns.ad_type,
    campaigns.funnel,
    adsinsights.campaign_id,
    SUM(
      adsinsights.impressions
    ) impressions,
    SUM(
      adsinsights.spend
    ) spend,
    SUM(
      adsinsights.clicks
    ) clicks,
    SUM(
      adsinsights.reach
    ) reach,
    SUM(
      adsinsights.link_click
    ) link_click,
    SUM(
      adsinsights.post_engagement
    ) post_engagement,
    SUM(
      adsinsights.offline_conversion_purchase
    ) offline_conversion_purchase,
    SUM(
      adsinsights.offline_conversion_purchase_value
    ) offline_conversion_purchase_value,
    SUM(
      adsinsights.pixel_purchase
    ) pixel_purchase,
    SUM(
      adsinsights.pixel_purchase_value
    ) pixel_purchase_value,
    SUM(
      adsinsights.meta_purchase
    ) meta_purchase,
    SUM(
      adsinsights.meta_purchase_value
    ) meta_purchase_value,
    SUM(
      adsinsights.messaging_conversation_started_7d
    ) _results_message
  FROM
    {{ ref('stg_facebookads__adsinsights') }}
    adsinsights
    LEFT JOIN {{ ref('stg_facebookads__campaigns') }}
    campaigns
    ON adsinsights.campaign_id = campaigns.campaign_id {{ dbt_utils.group_by(12) }}
)
SELECT
  DISTINCT facebook_performance.*
EXCEPT(
    page,
    pic
  ),
  COALESCE(
    s.new_ads_page,
    facebook_performance.page
  ) AS page,
  COALESCE(
    s.new_ads_pic,
    facebook_performance.pic
  ) AS pic
FROM
  facebook_performance
  LEFT JOIN {{ ref("dim__offline_stores") }}
  s
  ON facebook_performance.page = s.old_ads_page
