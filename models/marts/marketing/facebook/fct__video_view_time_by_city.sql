{{ config(
    materialized = 'view',
    tags = ['fact', 'view','facebook']
) }}

SELECT
    * except(city),
    split(post_id,"_")[0] as page_id,
    trim(SPLIT(city," - ")[safe_offset(1)]) AS country,
    trim(SPLIT(city," - ")[safe_offset(0)]) AS city,
FROM
    {{ ref("fct_fb__video_view_time_by_city") }}
