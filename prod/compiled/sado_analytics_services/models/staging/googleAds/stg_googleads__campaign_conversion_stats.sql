

SELECT
    stats.campaign_id,
    stats.segments_date AS date,
    stats.segments_conversion_action_name AS conversion_name,
    stats.segments_conversion_attribution_event_type AS conversion_attribution_type,
    stats.segments_slot AS slot,
    stats.metrics_conversions AS conversions,
    stats.metrics_conversions_value AS conversions_value,
FROM
    
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`GoogleAds`.`ads_CampaignConversionStats_1322374205`
                LIMIT
                    1000
            )
        

        
    stats