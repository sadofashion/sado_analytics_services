{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'event_date',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'insert_overwrite',
  unique_key = 'event_id',
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'daily','GA4']
) }}

SELECT
  DISTINCT event_id,
  event_date,
  event_timestamp,
  client_id,
  session_id,
  event_name,
  COALESCE(
    regexp_extract(
      page_location,
      r'(?:[a-zA-Z]+://)?(?:[a-zA-Z0-9-.]+){1}(/[^\?#;&]+)'
    ),
    '/'
  ) AS page_path,
  regexp_extract(
    page_location,
    r'\?([^#]*)'
  ) AS query_params,
  regexp_extract(
    page_location,
    r'#(.*)'
  ) AS fragment,
  link_click_target,
  page_location,
  page_referrer,
  payment_type,
  location,
  method,
  filter,
  transaction_id,
  shipping,
  delivery_method,
  store,
  value,
  form_value
FROM
  {{ ref('int_analytics__events_format') }}
WHERE
  1 = 1

{% if is_incremental() %}
AND (
  event_date >= DATE(_dbt_max_partition)
  OR event_date >= date_sub(CURRENT_DATE(), INTERVAL 2 DAY))
  {% endif %}
