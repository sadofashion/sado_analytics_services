{{ config(
    materialized = 'view',
    tags = ['fact', 'view','facebook']
) }}

SELECT
    * except(metric_value,city),
    safe_cast(metric_value as float64)/1000 as metric_value,
    trim(SPLIT(city," - ")[safe_offset(1)]) AS country,
    trim(SPLIT(city," - ")[safe_offset(0)]) AS city,
FROM
    {{ ref("fct_fb__video_view_time_by_city") }}
