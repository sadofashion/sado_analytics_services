{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'event_date',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'insert_overwrite',
  unique_key = 'event_id',
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'daily'],
) }}

{% set event_params = ['ga_session_id', 'page_location', 'page_referrer', 'click_element_url', 'payment_type', 'location', 'method', 'filter', 'transaction_id', 'referrer', 'shipping', 'delivery_method', 'store', 'value', 'form_value', 'ga_session_number', "engagement_time_msec", "search_term"] %}
{% set user_params = ['address','email','phone','name'] %}
WITH base AS (

  SELECT
    event_id,
    event_date,
    event_timestamp,
    client_id,
    event_name,
    continent,
    country,
    device_type,
    browser,
    operating_system,
    val_click_element_url AS link_click_target,
    COALESCE(
      traffic_source,
      "(none)"
    ) traffic_source,
    COALESCE(
      traffic_medium,
      "(none)"
    ) traffic_medium,
    COALESCE(
      traffic_campaign,
      "(none)"
    ) traffic_campaign,
    channel_grouping_session AS channel_grouping,
    CONCAT(
      val_ga_session_id,
      '-',
      client_id
    ) AS session_id,
    regexp_extract(
      val_page_location,
      r'utm_content=([^&]+)'
    ) AS traffic_referrer,
    {% for param in event_params if param != 'ga_session_id' %}
      val_{{ param }} AS {{ param }},
    {% endfor %}

    user_value,
    user_key,
  FROM
    (
      SELECT
        DISTINCT *
      EXCEPT(
          param_id,
          items
        )
      FROM
        {{ ref('stg_analytics__events') }}

{% if is_incremental() %}
WHERE
  event_date >= DATE(_dbt_max_partition)
  OR event_date >= date_sub(CURRENT_DATE(), INTERVAL 2 DAY)
{% endif %}) pivot(ANY_VALUE(param_value) AS val FOR param_key IN ('{{event_params | join("', '")}}'))
)
SELECT
  * {# {% for param in user_params %} #}
  {# user_{{param}}, #}
  {# {% endfor %} #}
FROM
  base pivot(ANY_VALUE(user_value) AS USER for user_key IN ('{{user_params | join("', '")}}'))
