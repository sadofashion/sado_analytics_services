

    SELECT
        
    
to_hex(md5(cast(coalesce(cast(event_timestamp as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(event_name as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(user_pseudo_id as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ARRAY_TO_STRING(ARRAY(SELECT CONCAT(p.key, "::", COALESCE(p.value.string_value, CAST(p.value.int_value AS STRING), CAST(p.value.float_value AS STRING), CAST(p.value.double_value AS STRING))) FROM UNNEST(event_params) AS p), "; ") as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS event_id,
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
        
        
            `agile-scheme-394814`.`analytics_336884118`.`events_*`
        

        

     WHERE
           _table_suffix LIKE 'intraday_%'
           
        OR (
           PARSE_DATE('%Y%m%d', _table_suffix) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
           
           )
           
