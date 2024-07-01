{{ config(
    materialized = 'view',
    tags = ['fact', 'view','facebook']
) }}

SELECT
    * except(metric_value),
    safe_cast(metric_value as float64)/1000 as metric_value,
FROM
    {{ ref("fct_fb__video_view_time_by_age_gender") }}
