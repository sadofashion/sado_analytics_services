{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'date_start',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'insert_overwrite',
  unique_key = ['date_start','campaign_id'],
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'hourly','fact']
) }}

WITH facebook_performance AS (

  SELECT
    campaigns.account_name,
    adsinsights.date_start,
    campaigns.campaign_name,
    campaigns.event_name as big_campaign,
    campaigns.ad_pic as pic,
    campaigns.content_edge as content_group,
    campaigns.ad_location as page,
    campaigns.content_edge as promoted_productline,
    campaigns.media_type,
    campaigns.campaign_category as ad_type,
    campaigns.audience_source_name as funnel,
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
    ON adsinsights.campaign_id = campaigns.campaign_id 
    where 1=1
    and campaigns.account_name not in ('Wookids_KT1','Woo kids _ KT1','Woo kids_KT2','Woo kids_KT3')
    {% if is_incremental() %}
       and date_start >= date_add(current_date, interval -1 day)

    {% endif %}

    {{ dbt_utils.group_by(12) }}
)
SELECT
  DISTINCT fb.*
EXCEPT(
    page,
    pic
  ),
  case 
    when s.local_page = fb.page then s.local_page
    when (s.region_page = fb.page and s.local_page <> fb.page) then fb.page
    else fb.page end as page,
  case 
    when s.local_page = fb.page then 'local_page'
    when (s.region_page = fb.page and s.local_page <> fb.page) then 'region_page'
    when fb.page IN (
        "5SFTHA",
        "5SFTIE",
        "5SFTUN",
        "5SFTRA",
        "5SFT",
        "5SFG",
        "5SF",
        "5SFTUY"
      ) then 'compiled'
    else 'others' end as page_type,
  coalesce(s.fb_ads_pic, fb.pic) as pic
FROM
  facebook_performance fb
  LEFT JOIN {{ ref("dim__branches") }}
  s
  ON (fb.page = s.local_page or (fb.page = s.region_page and fb.page <> s.region_page) ) and s.asm_name not in ('Online')
