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

{# WITH facebook_performance AS ( #}

  SELECT
    campaigns.account_id,
    adsinsights.date_start,
    {# campaigns.campaign_name, #}
    {# campaigns.event_name as big_campaign,
    campaigns.ad_pic as pic,
    campaigns.content_edge as content_group,
    campaigns.ad_location as page,
    campaigns.content_edge as promoted_productline,
    campaigns.media_type,
    campaigns.campaign_category as ad_type, #}
    {# campaigns.audience_source_name as funnel, #}
    adsinsights.ad_key,
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
    {% if is_incremental() %}
       and date_start >= date_add(current_date, interval -1 day)
    {% endif %}

    {{ dbt_utils.group_by(3) }}

