{{
  config(
    tags=['view', 'dimension','nhanhvn']
  )
}}

SELECT
    safe_cast(carriers.id as int64) as carrier_id,
    safe_cast(services.id as int64) as service_id,
    concat(carriers.name,' - ',services.name) as service_name
FROM
    {{ ref('base_nhanhvn__carriers') }} carriers,
    unnest(services) services
