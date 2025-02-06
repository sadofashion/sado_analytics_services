{{ config(
    materialized = 'view',
    tags = ['fb','dimensions','table']
) }}

SELECT
    *
FROM
    {{ ref("stg_fb__ad_accounts") }}
