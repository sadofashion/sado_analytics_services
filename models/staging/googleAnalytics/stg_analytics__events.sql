{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'event_date',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'insert_overwrite',
  unique_key = 'param_id',
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'daily','GA4']
) }}
  SELECT
  {{ dbt_utils.generate_surrogate_key(['event_id', 'params.key']) }} AS param_id,
  event_id,
  event_date,
  event_timestamp,
  client_id,
  event_name,
  geo.continent AS continent,
  geo.country AS country,
  device.category AS device_type,
  device.web_info.browser AS browser,
  device.operating_system AS operating_system,
  traffic_source.source AS traffic_source,
  traffic_source.name AS traffic_campaign,
  traffic_source.medium AS traffic_medium,
  CASE
    WHEN traffic_source.source = '(direct)'
    AND (traffic_source.medium IN ('(not set)', '(none)')
    OR traffic_source.medium IS NULL) THEN 'Direct'
    WHEN regexp_contains(
      traffic_source.name,
      'cross-network'
    ) THEN 'Cross-network'
    WHEN (
      regexp_contains(
        traffic_source.source,
        'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart'
      )
      OR regexp_contains(
        traffic_source.name,
        '^(.*(([^a-df-z]|^)shop|shopping).*)$'
      )
    )
    AND regexp_contains(
      traffic_source.medium,
      '^(.*cp.*|ppc|paid.*)$'
    ) THEN 'Paid Shopping'
    WHEN regexp_contains(
      traffic_source.source,
      'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex'
    )
    AND regexp_contains(
      traffic_source.medium,
      '^(.*cp.*|ppc|paid.*)$'
    ) THEN 'Paid Search'
    WHEN regexp_contains(
      traffic_source.source,
      'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp'
    )
    AND regexp_contains(
      traffic_source.medium,
      '^(.*cp.*|ppc|paid.*)$'
    ) THEN 'Paid Social'
    WHEN regexp_contains(
      traffic_source.source,
      'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube'
    )
    AND regexp_contains(
      traffic_source.medium,
      '^(.*cp.*|ppc|paid.*)$'
    ) THEN 'Paid Video'
    WHEN traffic_source.medium IN (
      'display',
      'banner',
      'expandable',
      'interstitial',
      'cpm'
    ) THEN 'Display'
    WHEN regexp_contains(
      traffic_source.source,
      'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart'
    )
    OR regexp_contains(
      traffic_source.name,
      '^(.*(([^a-df-z]|^)shop|shopping).*)$'
    ) THEN 'Organic Shopping'
    WHEN regexp_contains(
      traffic_source.source,
      'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp|zalo'
    )
    OR traffic_source.medium IN (
      'social',
      'social-network',
      'social-media',
      'sm',
      'social network',
      'social media','zalo'
    ) THEN 'Organic Social'
    WHEN regexp_contains(
      traffic_source.source,
      'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube'
    )
    OR regexp_contains(
      traffic_source.medium,
      '^(.*video.*)$'
    ) THEN 'Organic Video'
    WHEN regexp_contains(
      traffic_source.source,
      'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex'
    )
    OR traffic_source.medium = 'organic' THEN 'Organic Search'
    WHEN regexp_contains(
      traffic_source.source,
      'email|e-mail|e_mail|e mail'
    )
    OR regexp_contains(
      traffic_source.medium,
      'email|e-mail|e_mail|e mail'
    ) THEN 'Email'
    WHEN traffic_source.medium = 'affiliate' THEN 'Affiliates'
    WHEN traffic_source.medium = 'referral' THEN 'Referral'
    WHEN traffic_source.medium = 'audio' THEN 'Audio'
    WHEN traffic_source.medium = 'sms' THEN 'SMS'
    WHEN traffic_source.medium LIKE '%push'
    OR regexp_contains(
      traffic_source.medium,
      'mobile|notification'
    ) THEN 'Mobile Push Notifications'
    ELSE 'Unassigned'
  END AS channel_grouping_session,
  params.key AS param_key,
  COALESCE(
    params.value.string_value,
    CAST(
      params.value.int_value AS STRING
    ),
    CAST(
      params.value.float_value AS STRING
    ),
    CAST(
      params.value.double_value AS STRING
    )
  ) AS param_value,
  items,
  USER.key AS user_key,
  COALESCE(
    USER.value.string_value,
    CAST(
      USER.value.int_value AS STRING
    ),
    CAST(
      USER.value.float_value AS STRING
    ),
    CAST(
      USER.value.double_value AS STRING
    )
  ) AS user_value,
FROM
  (
    SELECT
      *
    FROM
      {{ ref('base_analytics__events') }}

{% if is_incremental() %}
WHERE
  event_date >= DATE(_dbt_max_partition)
  OR event_date >= date_sub(CURRENT_DATE(), INTERVAL 1 DAY)
{% endif %})
LEFT JOIN unnest(event_params) AS params
LEFT JOIN unnest(user_properties) USER
