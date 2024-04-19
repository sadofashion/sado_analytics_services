SELECT
        DISTINCT ad_group_id,
        campaign_id,
        customer_id AS account_id,
        ad_group_type,
        FIRST_VALUE(ad_group_name) over(
            PARTITION BY ad_group_id,
            campaign_id
            ORDER BY _DATA_DATE desc, _LATEST_DATE desc DESC
        ) AS ad_group_name,
    FROM
        {{ source(
            'googleads',
            'ad_group'
        ) }}