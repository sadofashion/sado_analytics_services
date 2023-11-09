{{
  config(
    materialized = 'incremental',
    partition_by = {
      'field': 'date_start',
      'data_type': 'date',
      'granularity': 'day'},
    incremental_strategy = 'insert_overwrite',
    unique_key = 'row_id',
    on_schema_change = 'sync_all_columns',
    tags=['incremental', 'daily']
  )
}}


SELECT
{{ dbt_utils.generate_surrogate_key(['account_id','date_start','region']) }} as row_id,
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
    {{dbt_utils.group_by(4)}}