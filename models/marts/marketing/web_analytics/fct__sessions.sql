{{ config(
    materialized = 'view',
    tags = ['fact', 'view','ga4']
) }}

SELECT
    s.*
FROM
    {{ ref("fct_ga4__sessions") }} s
left join {{ ref('dim__excluded_clients') }} ex on s.client_key = ex.client_key
where ex.client_key is null