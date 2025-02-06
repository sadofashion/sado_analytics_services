{{ config(
    materialized = 'view',
    tags = ['fact', 'view','ga4']
) }}

SELECT
    *
FROM
    {{ ref("fct_ga4__pages") }}
