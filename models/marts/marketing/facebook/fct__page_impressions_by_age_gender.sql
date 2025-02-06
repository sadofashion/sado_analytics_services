{{ config(
    materialized = 'view',
    tags = ['fact', 'view','facebook']
) }}

SELECT
    *
FROM
    {{ ref("fct_fb__page_impressions_by_age_gender") }}
