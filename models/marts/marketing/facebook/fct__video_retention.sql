{{ config(
    materialized = 'view',
    tags = ['fact', 'view','facebook']
) }}

SELECT
    * except(metric_value,second),
    safe_cast(second as int64) as second,
    safe_cast(metric_value as float64) as metric_value,
FROM
    {{ ref("fct_fb__video_retention") }}
