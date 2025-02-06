{{ config(
    tags = ['ggads','fact','table']
) }}

WITH ad_group_stats AS (

    SELECT
        ad_group_id,
        campaign_id,
        DATE,
        SUM(clicks) clicks,
        SUM(conversions) conversions,
        SUM(conversions_value) conversions_value,
        SUM(cost) cost,
        SUM(impressions) impressions,
        SUM(interactions) interactions,
        SUM(view_through_conversions) view_through_conversions,
    FROM
        {{ ref('stg_googleads__ad_group_stats') }}
    GROUP BY
        1,
        2,
        3
)
SELECT
    ags.campaign_id,
    ags.ad_group_id,
    ags.date,
    ags.network_type,
    ags.top_impression_percentage,
    ags.search_top_impression_share,
    ags.video_quartile_p100_rate,
    ags.video_quartile_p75_rate,
    ags.video_quartile_p50_rate,
    ags.video_quartile_p25_rate,
    ags.absolute_top_impression_percentage,
    ags.content_impression_share,
    ags.content_rank_lost_impression_share,
    ags.cross_device_conversions,
    ags.engagements,
    ags.relative_ctr,
    ags.search_absolute_top_impression_share,
    ags.search_budget_lost_absolute_top_impression_share,
    ags.search_budget_lost_top_impression_share,
    ags.search_impression_share,
    ags.search_rank_lost_absolute_top_impression_share,
    ags.search_rank_lost_impression_share,
    ags.search_rank_lost_top_impression_share,
    ags.video_views,
    s.clicks,
    s.conversions,
    s.cost,
    s.impressions,
    s.interactions,
    s.view_through_conversions,
FROM
    {{ ref('stg_googleads__ad_group_cross_device_stats') }}
    ags
    LEFT JOIN ad_group_stats s
    ON ags.campaign_id = s.campaign_id
    AND ags.ad_group_id = s.ad_group_id
    AND ags.date = s.date
