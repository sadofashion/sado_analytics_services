{{ config(
    materialized = 'view',
    tags = ['ggads','fact','view']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['campaign_id','segments_date','segments_device','segments_ad_network_type','segments_slot']) }} AS row_id,
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
    {{ source(
        'googleads',
        'campaign_basic_stats'
    ) }}
    stats