{{ config(
    materialized = 'view',
    tags = ['dimension', 'view','ga4']
) }}

SELECT
    *
FROM
    {{ ref("dim_ga4__client_keys") }}
