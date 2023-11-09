{{ config(
    materialized='view',
    tags=['ggads','fact','view']
)}}

SELECT
    stats.campaign_id,
    stats.segments_date AS date,
    stats.segments_conversion_action_name AS conversion_name,
    stats.segments_conversion_attribution_event_type AS conversion_attribution_type,
    stats.segments_slot AS slot,
    stats.metrics_conversions AS conversions,
    stats.metrics_conversions_value AS conversions_value,
FROM
    {{ source(
        'googleads',
        'campaign_conversion_stats'
    ) }}
    stats