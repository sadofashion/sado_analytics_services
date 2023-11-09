SELECT
        DISTINCT ad_group_id,
        campaign_id,
        customer_id AS account_id,
        ad_group_type,
        FIRST_VALUE(ad_group_name) over(
            PARTITION BY ad_group_id,
            campaign_id
            ORDER BY
                _LATEST_DATE DESC
        ) AS ad_group_name,
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`GoogleAds`.`ads_AdGroup_1322374205`
                LIMIT
                    1000
            )
        

        