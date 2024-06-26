{{ config(
    materialized = 'view',
    tags = ['fact', 'view','facebook']
) }}

SELECT
    * except(metric_value,second),
    safe_cast(second as int64) as second,
    safe_cast(metric_value as float64) as metric_value,
    split(post_id,"_")[safe_offset(0)] as page_id,
FROM
    {{ ref("fct_fb__video_retention") }}
