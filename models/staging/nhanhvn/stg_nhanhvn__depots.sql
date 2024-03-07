{{
  config(
    tags=['view', 'dimension','nhanhvn']
  )
}}

WITH source AS (
  {{ dbt_utils.deduplicate(
    relation = source(
        'nhanhvn',
        'p_stores_*'
    ),
    partition_by = 'id',
    order_by = "_batched_at desc",
) }}
)

SELECT
    safe_cast(id as int64) AS depot_id,
    NAME AS depot_name,
    mobile AS contact_number,
    cityName AS city,
    districtName AS district,
    address
FROM
    source