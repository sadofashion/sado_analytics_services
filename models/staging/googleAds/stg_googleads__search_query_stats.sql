{{ config(
    materialized = 'view',
    tags = ['ggads','fact','view']
) }}

SELECT
    segments_date AS DATE,
    ad_group_id,
    campaign_id,
    search_term_view_search_term AS search_term,
    segments_ad_network_type AS network_type,
    segments_search_term_match_type AS match_type,
    search_term_view_status AS view_status,
    AVG(metrics_absolute_top_impression_percentage) absolute_top_impression_percentage,
    SUM(metrics_all_conversions) all_conversions,
    AVG(metrics_all_conversions_from_interactions_rate) all_conversions_from_interactions_rate,
    SUM(metrics_all_conversions_value) all_conversions_value,
    SUM(metrics_clicks) clicks,
    SUM(metrics_conversions) conversions,
    AVG(metrics_conversions_from_interactions_rate) conversions_from_interactions_rate,
    SUM(metrics_conversions_value) conversions_value,
    SUM(
        metrics_cost_micros / 1e6
    ) cost,
    AVG(metrics_cost_per_all_conversions) cost_per_all_conversions,
    AVG(metrics_cost_per_conversion) cost_per_conversion,
    SUM(metrics_cross_device_conversions) cross_device_conversions,
    SUM(metrics_engagements) engagements,
    SUM(metrics_impressions) impressions,
    SUM(metrics_interactions) interactions,
    AVG(metrics_top_impression_percentage) top_impression_percentage,
    SUM(metrics_value_per_all_conversions) value_per_all_conversions,
    SUM(metrics_value_per_conversion) value_per_conversion,
    AVG(metrics_video_quartile_p100_rate) video_quartile_p100_rate,
    AVG(metrics_video_quartile_p25_rate) video_quartile_p25_rate,
    AVG(metrics_video_quartile_p50_rate) video_quartile_p50_rate,
    AVG(metrics_video_quartile_p75_rate) video_quartile_p75_rate,
    SUM(metrics_video_views) video_views,
    SUM(metrics_view_through_conversions) view_through_conversions,
FROM
    {{ source(
        'googleads',
        'search_query_stats'
    ) }}
WHERE
    1 = 1 {{ dbt_utils.group_by(7) }}
