{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'date_start',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'insert_overwrite',
  unique_key = 'row_id',
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'daily']
) }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['account_id','date_start','region']) }} AS row_id,
  account_id,
  date_start,
  region,
  SUM(impressions) impressions,
  SUM(spend) spend,
  SUM(clicks) clicks,
  SUM(reach) reach,
  SUM(link_click) link_click,
  SUM(post_engagement) post_engagement,
  SUM(offline_conversion_purchase) offline_conversion_purchase,
  SUM(offline_conversion_purchase_value) offline_conversion_purchase_value,
  SUM(pixel_purchase) pixel_purchase,
  SUM(pixel_purchase_value) pixel_purchase_value,
  SUM(messaging_conversation_started_7d) messaging_conversation_started_7d
FROM
  {{ ref('stg_facebookads__regioninsights') }}
  {{ dbt_utils.group_by(4) }}
