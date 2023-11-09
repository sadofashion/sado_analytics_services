

SELECT
    stats.ad_group_id,
    stats.segments_date AS date,
    stats.campaign_id,
    stats.segments_ad_network_type AS network_type,
    stats.metrics_top_impression_percentage AS top_impression_percentage,
    stats.metrics_search_top_impression_share AS search_top_impression_share,
    stats.metrics_video_quartile_p100_rate AS video_quartile_p100_rate,
    stats.metrics_video_quartile_p75_rate AS video_quartile_p75_rate,
    stats.metrics_video_quartile_p50_rate AS video_quartile_p50_rate,
    stats.metrics_video_quartile_p25_rate AS video_quartile_p25_rate,
    stats.metrics_absolute_top_impression_percentage AS absolute_top_impression_percentage,
    stats.metrics_content_impression_share AS content_impression_share,
    stats.metrics_content_rank_lost_impression_share AS content_rank_lost_impression_share,
    stats.metrics_cross_device_conversions AS cross_device_conversions,
    stats.metrics_engagements AS engagements,
    stats.metrics_relative_ctr AS relative_ctr,
    stats.metrics_search_absolute_top_impression_share AS search_absolute_top_impression_share,
    stats.metrics_search_budget_lost_absolute_top_impression_share AS search_budget_lost_absolute_top_impression_share,
    stats.metrics_search_budget_lost_top_impression_share AS search_budget_lost_top_impression_share,
    stats.metrics_search_impression_share AS search_impression_share,
    stats.metrics_search_rank_lost_absolute_top_impression_share AS search_rank_lost_absolute_top_impression_share,
    stats.metrics_search_rank_lost_impression_share AS search_rank_lost_impression_share,
    stats.metrics_search_rank_lost_top_impression_share AS search_rank_lost_top_impression_share,
    stats.metrics_video_views AS video_views,
FROM
    
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`GoogleAds`.`ads_AdGroupCrossDeviceStats_1322374205`
                LIMIT
                    1000
            )
        

        
    stats