{{
  config(
    tags=['view', 'dimension','nhanhvn']
  )
}}

SELECT
    safe_cast(id as int64) AS depot_id,
    NAME AS depot_name,
    mobile AS contact_number,
    cityName AS city,
    districtName AS district,
    address
FROM
    {{ ref('base_nhanhvn__stores') }}
