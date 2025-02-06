{{ config(
    materialized = 'view',
    tags = ['fact', 'view','ga4']
) }}

SELECT
    sd.*
FROM
    {{ ref("fct_ga4__sessions_daily") }} sd
    left join {{ ref('dim__excluded_clients') }} ex on sd.client_key = ex.client_key
where ex.client_key is null
