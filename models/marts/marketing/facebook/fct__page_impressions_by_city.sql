{{ config(
    materialized = 'view',
    tags = ['fact', 'view','facebook']
) }}

SELECT
    * except(city),
    trim(SPLIT(city," - ")[safe_offset(1)]) AS country,
    trim(SPLIT(city," - ")[safe_offset(0)]) AS city,
FROM
    {{ ref("fct_fb__page_impressions_by_city") }}
