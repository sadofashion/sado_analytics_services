{{ config(
    materialized = 'view',
    tags = ['dimension', 'view','facebook']
) }}

SELECT
    *
FROM
    {{ ref("dim_fb__posts") }}
