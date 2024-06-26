{{ config(
    materialized = 'view',
    tags = ['fact', 'view','facebook']
) }}

SELECT
    * except(metric_time), 
    split(post_id,"_")[safe_offset(0)] as page_id,
    date_add(metric_time, interval 1 day) as metric_time,
FROM
    {{ ref("fct_fb__post_insights") }}