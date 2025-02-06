{{ config(
    materialized = 'view',
) }}

SELECT
    local_page_code,
    local_page,
    region,
    province,
    asm_name,
    fb_ads_pic,
    MAX(opening_day) opening_day,
    MAX(close_date) close_date,
FROM
    {{ ref('dim__branches') }}
WHERE
    1=1 
    and channel = 'Offline' 
    and local_page_code is not null
    {{ dbt_utils.group_by(6) }}

