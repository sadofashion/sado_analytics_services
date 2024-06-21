{{ config(
    materialized = 'view',
    tags = ['fact', 'view','facebook']
) }}

SELECT
    *
FROM
    {{ ref("fct_fb__post_insights") }}
