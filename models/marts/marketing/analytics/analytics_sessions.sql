{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'session_date',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'insert_overwrite',
  unique_key = 'session_id',
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'daily','GA4']
) }}

SELECT
  DISTINCT session_id,
  client_id,
  FIRST_VALUE(
    channel_grouping ignore nulls
  ) over (session_window) AS channel_grouping,
  FIRST_VALUE(
    ga_session_number ignore nulls
  ) over (session_window) AS ga_session_number,
  LAST_VALUE(continent) over(session_window) AS continent,
  LAST_VALUE(country) over(session_window) AS country,
  LAST_VALUE(device_type) over(session_window) AS device_type,
  LAST_VALUE(browser) over(session_window) AS browser,
  LAST_VALUE(operating_system) over(session_window) AS operating_system,
  FIRST_VALUE(event_date) over(session_window) AS session_date,
  FIRST_VALUE(event_timestamp) over(session_window) AS session_initiated,
  FIRST_VALUE(page_location) over(session_window) AS landing_page,
  LAST_VALUE(page_location) over(session_window) AS exit_page,
  FIRST_VALUE(traffic_source) over(session_window) AS traffic_source,
  FIRST_VALUE(traffic_campaign) over(session_window) AS traffic_campaign,
  FIRST_VALUE(traffic_medium) over(session_window) AS traffic_medium,
  FIRST_VALUE(traffic_referrer) over(session_window) AS traffic_referrer,
  SUM(CAST(engagement_time_msec AS int64) / 1000) over(session_window) AS engagement_time,
FROM
  {{ ref('int_analytics__events_format') }}

{% if is_incremental() %}
qualify FIRST_VALUE(event_date) over(session_window) >= DATE(_dbt_max_partition)
OR FIRST_VALUE(event_date) over(session_window) >= date_sub(CURRENT_DATE(), INTERVAL 2 DAY)
{% endif %}

window session_window AS (
  PARTITION BY session_id
  ORDER BY
    event_timestamp rows BETWEEN unbounded preceding
    AND unbounded following
)
