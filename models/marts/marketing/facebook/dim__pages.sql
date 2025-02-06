{{ config(
    materialized = 'view',
    tags = ['dimension', 'view','facebook']
) }}

SELECT
    p.*,
    lg.region,
    lg.province,
    lg.fb_ads_pic,
FROM
    {{ ref("stg_fb__pages") }}
    p
    LEFT JOIN {{ ref("dim__location_groups") }}
    lg
    ON p.store_code = lg.local_page_code
