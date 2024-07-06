{{ config(
    materialized = 'view',
    tags = ['fact', 'view','facebook']
) }}

SELECT
    * 
    {# except(metric_time),  #}
FROM
    {{ ref("fct_fb__post_insights") }}