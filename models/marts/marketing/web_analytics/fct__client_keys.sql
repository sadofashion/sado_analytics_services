{{ config(
    materialized = 'view',
    tags = ['fact', 'view','ga4']
) }}

SELECT
    cl.*
FROM
    {{ ref("fct_ga4__client_keys") }} cl 
left join {{ ref('dim__excluded_clients') }} ex on cl.client_key = ex.client_key
where ex.client_key is null