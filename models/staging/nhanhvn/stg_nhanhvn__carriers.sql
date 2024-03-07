{{ config(
  tags = ['view', 'dimension','nhanhvn']
) }}

WITH source AS (
  {{ dbt_utils.deduplicate(
    relation = source(
      'nhanhvn',
      'p_carriers_*'
    ),
    partition_by = 'id',
    order_by = "_batched_at desc",
  ) }}
)
SELECT
  safe_cast(
    carriers.id AS int64
  ) AS carrier_id,
  safe_cast(
    services.id AS int64
  ) AS service_id,
  CONCAT(
    carriers.name,
    ' - ',
    services.name
  ) AS service_name
FROM
  source carriers,
  unnest(services) services
