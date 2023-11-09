{{
  config(
    materialized = 'incremental',
    partition_by = {
      'field': 'event_date', 
      'data_type': 'date', 
      'granularity': 'day'},
    incremental_strategy = 'insert_overwrite',
    unique_key='event_id',
    on_schema_change = 'sync_all_column',
    tags=['incremental', 'daily']
  )
}}

    SELECT
        {{ dbt_utils.generate_surrogate_key([
           'event_timestamp',
           'event_name',
           'user_pseudo_id',
           'ARRAY_TO_STRING(ARRAY(SELECT CONCAT(p.key, "::", COALESCE(p.value.string_value, CAST(p.value.int_value AS STRING), CAST(p.value.float_value AS STRING), CAST(p.value.double_value AS STRING))) FROM UNNEST(event_params) AS p), "; ")'
        ]) }} AS event_id,
        PARSE_DATE('%Y%m%d', event_date) AS event_date, 
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
        event_name,
        event_params,
        event_previous_timestamp,
        event_value_in_usd,
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_pseudo_id AS client_id,
        privacy_info,
        user_properties, 
        user_first_touch_timestamp,
        user_ltv, 
        device, 
        geo, 
        app_info,
        traffic_source,
        stream_id,
        platform,
        event_dimensions,
        ecommerce, 
        items,
      FROM
        {{ source('analytics', 'events') }}
{% if is_incremental() %}
     WHERE
           _table_suffix LIKE 'intraday_%'
           
        OR (
           PARSE_DATE('%Y%m%d', _table_suffix) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
           {% if target.name == 'prod' %}
           OR 
              PARSE_DATE('%Y%m%d', _table_suffix) >= DATE(_dbt_max_partition)
              {% endif %}
           )
           
{% endif %}