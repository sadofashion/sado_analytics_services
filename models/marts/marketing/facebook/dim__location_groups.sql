{{ config(
    materialized = 'view',
) }}

SELECT
    DISTINCT local_page_code,
    local_page,
    region,
    province,
    asm_name,
    fb_ads_pic
FROM
    {{ ref('dim__branches') }}
    where channel = 'Offline'
