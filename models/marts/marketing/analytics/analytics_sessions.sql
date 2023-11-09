{{
  config(
    materialized = 'incremental',
    partition_by = {
      'field': 'session_date',
      'data_type': 'date',
      'granularity': 'day'},
    incremental_strategy = 'insert_overwrite',
    unique_key = 'session_id',
    on_schema_change = 'sync_all_columns',
    tags=['incremental', 'daily','GA4']
  )
}}

    SELECT 
  DISTINCT 
        session_id,
        client_id,
        FIRST_VALUE(channel_grouping ignore nulls) over (session_window) as channel_grouping,
        FIRST_VALUE(ga_session_number ignore nulls) over (session_window) as ga_session_number,
        LAST_VALUE(continent) OVER(session_window) AS continent,
        LAST_VALUE(country) OVER(session_window) AS country,
        LAST_VALUE(device_type) OVER(session_window) AS device_type,
        LAST_VALUE(browser) OVER(session_window) AS browser,
        LAST_VALUE(operating_system) OVER(session_window) AS operating_system,
        FIRST_VALUE(event_date) OVER(session_window) AS session_date,
        FIRST_VALUE(event_timestamp) OVER(session_window) AS session_initiated,
        FIRST_VALUE(page_location) OVER(session_window) AS landing_page, 
        LAST_VALUE(page_location) OVER(session_window) AS exit_page,
        FIRST_VALUE(traffic_source) OVER(session_window) AS traffic_source,
        FIRST_VALUE(traffic_campaign) OVER(session_window) AS traffic_campaign,
        FIRST_VALUE(traffic_medium) OVER(session_window) AS traffic_medium,
        FIRST_VALUE(traffic_referrer) OVER(session_window) AS traffic_referrer,
        sum(cast(engagement_time_msec as int64) /1000) OVER(session_window) AS engagement_time, 
      FROM
        {{ ref('int_analytics__events_format') }}
{% if is_incremental() %}
   QUALIFY
           FIRST_VALUE(event_date) OVER(session_window) >= DATE(_dbt_max_partition)
        OR FIRST_VALUE(event_date) OVER(session_window) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
{% endif %}
    WINDOW 
        session_window AS (
            PARTITION BY session_id
                ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )    