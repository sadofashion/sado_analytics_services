{{ config(
    materialized='view',
    tags=['ggads','fact','view']
)}}
SELECT
    stats.segments_date AS date,
    stats.ad_group_id,
    stats.campaign_id,
    stats.segments_device AS device,
    stats.segments_slot AS slot,
    stats.segments_ad_network_type AS network_type,
    keyword.ad_group_criterion_keyword_match_type AS match_type,
    keyword.ad_group_criterion_keyword_text as keyword,
    SUM(
        stats.metrics_clicks
    ) AS clicks,
    SUM(
        stats.metrics_conversions
    ) AS conversions,
    SUM(
        stats.metrics_conversions_value
    ) AS conversions_value,
    SUM(
        stats.metrics_cost_micros / 1e6
    ) AS cost,
    SUM(
        stats.metrics_impressions
    ) AS impressions,
    SUM(
        stats.metrics_interactions
    ) AS interactions,
    SUM(
        stats.metrics_view_through_conversions
    ) AS view_through_conversions,
FROM
    {{ source(
        'googleads',
        'keyword_basic_stats'
    ) }}
    stats
    LEFT JOIN {{ source(
        'googleads',
        'keyword'
    ) }}
    keyword
    ON stats.ad_group_criterion_criterion_id = keyword.ad_group_criterion_criterion_id
    AND stats.ad_group_id = keyword.ad_group_id
WHERE 1=1
    and keyword._LATEST_DATE = keyword._DATA_DATE
    {{dbt_utils.group_by(8)}}