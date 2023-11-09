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
        {{ source(
            'googleads',
            'ad_group'
        ) }}