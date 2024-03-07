{{
  config(
    tags=['view', 'dimension','nhanhvn']
  )
}}


WITH source AS (
  {{ dbt_utils.deduplicate(
    relation = source(
        'nhanhvn',
        'p_users_*'
    ),
    partition_by = 'id',
    order_by = "_batched_at desc",
) }}
)

SELECT
    safe_cast(id as int64) AS user_id,
    userName AS user_name,
    email AS email,
    mobile AS contact_number,
    roleName AS role,
FROM
    source
