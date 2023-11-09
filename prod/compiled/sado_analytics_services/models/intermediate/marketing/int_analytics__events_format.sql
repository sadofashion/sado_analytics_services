

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
        val_page_location AS page_location,
        val_page_referrer AS page_referrer,
        CONCAT(val_ga_session_id, '-', client_id) AS session_id,
        val_click_element_url AS link_click_target,
        coalesce(traffic_source,"(none)") traffic_source,
        coalesce(traffic_medium,"(none)") traffic_medium,
        coalesce(traffic_campaign,"(none)") traffic_campaign,
        REGEXP_EXTRACT(val_page_location, 'utm_content=([^&]+)') AS traffic_referrer,
        channel_grouping_session as channel_grouping,
        val_payment_type as payment_type,
        val_location as location,
        val_method as method,
        val_filter as filter,
        val_transaction_id as transaction_id,
        val_shipping as shipping,
        val_delivery_method as delivery_method,
        val_store as store,
        val_value as value,
        val_form_value as form_value,
        val_ga_session_number as ga_session_number,
        val_engagement_time_msec as engagement_time_msec,
        user_value,user_key,
        val_search_term as search_term,
      FROM ( 
               SELECT 
             DISTINCT 
                   * EXCEPT(param_id,items)
                 FROM
                   `agile-scheme-394814`.`dbt_dev`.`stg_analytics__events`

                WHERE
                      event_date >= DATE(_dbt_max_partition)

                   OR event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)

     )
     PIVOT(

        ANY_VALUE(param_value) AS val
        FOR param_key IN (
          'ga_session_id',
          'page_location',
          'page_referrer',
          'click_element_url',
          'payment_type',
          'location',
          'method',
          'filter',
          'transaction_id',
          'referrer',
          'shipping',
          'delivery_method',
          'store',
          'value',
          'form_value',
          'ga_session_number',
          "engagement_time_msec",
          "search_term"
        )
     )