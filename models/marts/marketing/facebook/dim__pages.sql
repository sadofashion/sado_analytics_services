{{ config(
    materialized = 'view',
    tags = ['dimension', 'view','facebook']
) }}

SELECT
    *
FROM
    {{ ref("stg_fb__pages") }}
