{{ config(
    materialized = 'view',
    tags = ['fact', 'view','facebook']
) }}

SELECT
    *,
    split(post_id,"_")[0] as page_id,
FROM
    {{ ref("fct_fb__video_view_time_by_age_gender") }}
