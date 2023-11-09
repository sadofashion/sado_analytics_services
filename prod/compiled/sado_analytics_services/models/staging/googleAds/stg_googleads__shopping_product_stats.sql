

SELECT
    segments_date AS date,
    ad_group_id,
    campaign_id,
    upper(segments_product_item_id) item_id,
    SUM(metrics_clicks) clicks,
    SUM(metrics_conversions) conversions,
    AVG(metrics_conversions_from_interactions_rate) conversions_from_interactions_rate,
    SUM(metrics_conversions_value) conversions_value,
    SUM(
        metrics_cost_micros / 1e6
    ) cost,
    AVG(metrics_cost_per_conversion) cost_per_conversion,
    SUM(metrics_cross_device_conversions) cross_device_conversions,
    SUM(metrics_impressions) impressions,
FROM
    
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`GoogleAds`.`ads_ShoppingProductStats_1322374205`
                LIMIT
                    1000
            )
        

        
    group by 1,2,3,4