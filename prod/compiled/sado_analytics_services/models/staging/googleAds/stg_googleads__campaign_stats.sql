

SELECT
    
    
to_hex(md5(cast(coalesce(cast(campaign_id as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(segments_date as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(segments_device as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(segments_ad_network_type as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(segments_slot as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS row_id,
    stats.campaign_id,
    stats.segments_date AS date,
    stats.segments_device AS device,
    stats.segments_ad_network_type AS network_type,
    stats.segments_slot AS slot,
    stats.metrics_clicks AS clicks,
    stats.metrics_conversions AS conversions,
    stats.metrics_conversions_value AS conversions_value,
    stats.metrics_cost_micros / 1e6 AS cost,
    stats.metrics_impressions AS impressions,
    stats.metrics_interactions AS interactions,
    stats.metrics_view_through_conversions AS view_through_conversions,
FROM
    
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`GoogleAds`.`ads_CampaignBasicStats_1322374205`
                LIMIT
                    1000
            )
        

        
    stats