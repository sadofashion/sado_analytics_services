{{ config(
    materialized = 'view',
    tags = ['dimension', 'view','ga4']
) }}

SELECT
    *
FROM
    {{ ref("dim_ga4__sessions") }} s
    where landing_page_hostname in (
        '5sfashion.vn',
        'www.5sfashion.vn',
        'khaosat.5sfashion.vn',
        'vqmm.5sfashion.com.vn'
    )