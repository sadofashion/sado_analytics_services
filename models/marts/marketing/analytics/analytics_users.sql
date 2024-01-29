{{ config(
    materialized = 'incremental',
    partition_by ={ 'field': 'first_visit_date',
    'data_type': 'date',
    'granularity': 'day' },
    incremental_strategy = 'merge',
    unique_key = 'client_id',
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'daily','GA4','users','fact']
) }}

WITH raw_ AS (

    SELECT
        DISTINCT client_id,
        FIRST_VALUE(
            user_phone ignore nulls
        ) over(client_window) AS user_phone,
        FIRST_VALUE(
            user_email ignore nulls
        ) over(client_window) AS user_email,
        FIRST_VALUE(
            user_name ignore nulls
        ) over(client_window) AS user_name,
        FIRST_VALUE(
            user_address ignore nulls
        ) over(client_window) AS user_address,
        channel_grouping,
        session_id,
        country,
        device_type,
        browser,
        operating_system,
        event_date,
        event_timestamp,
        traffic_source,
        traffic_campaign,
        traffic_medium,
    FROM
        {{ ref('int_analytics__events_format') }}

{% if is_incremental() %}
WHERE
    event_date >= DATE(_dbt_max_partition)
    OR event_date >= date_sub(CURRENT_DATE(), INTERVAL 2 DAY)
{% endif %}

{% if is_incremental() %}
qualify FIRST_VALUE(event_date) over(client_window) >= DATE(_dbt_max_partition)
OR FIRST_VALUE(event_date) over(client_window) >= date_sub(CURRENT_DATE(), INTERVAL 2 DAY)
{% endif %}

window client_window AS (
    PARTITION BY client_id
    ORDER BY
        event_timestamp rows BETWEEN unbounded preceding
        AND unbounded following
)
)
SELECT
    DISTINCT client_id,
    user_phone,
    user_email,
    user_name,
    user_address,
    FIRST_VALUE(channel_grouping) over(user_window) AS first_channel_grouping,
    FIRST_VALUE(session_id) over(user_window) AS first_session_id,
    LAST_VALUE(session_id) over(user_window) AS last_session_id,
    LAST_VALUE(country) over(user_window) AS country,
    LAST_VALUE(device_type) over(user_window) AS device_type,
    LAST_VALUE(browser) over(user_window) AS browser,
    LAST_VALUE(operating_system) over(user_window) AS operating_system,
    FIRST_VALUE(event_date) over(user_window) AS first_visit_date,
    FIRST_VALUE(event_timestamp) over(user_window) AS first_visit_initiated,
    FIRST_VALUE(traffic_source) over(user_window) AS first_traffic_source,
    FIRST_VALUE(traffic_campaign) over(user_window) AS first_traffic_campaign,
    FIRST_VALUE(traffic_medium) over(user_window) AS first_traffic_medium,
FROM
    raw_

{% if is_incremental() %}
qualify FIRST_VALUE(event_date) over(user_window) >= DATE(_dbt_max_partition)
OR FIRST_VALUE(event_date) over(user_window) >= date_sub(CURRENT_DATE(), INTERVAL 2 DAY)
{% endif %}

window user_window AS (PARTITION BY COALESCE(user_phone, client_id)
ORDER BY
    event_timestamp rows BETWEEN unbounded preceding
    AND unbounded following)
