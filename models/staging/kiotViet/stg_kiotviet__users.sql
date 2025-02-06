{{
  config(
    tags=['view', 'dimension','kiotviet']
  )
}}

WITH source AS (
        {{ dbt_utils.deduplicate(
        relation = source(
            'kiotViet',
            'p_users_list'
        ),
        partition_by = 'id',
        order_by = "_batched_at desc",
    ) }}
)


SELECT
    id as user_id,
    userName as user_name,
    givenName as given_name,
    birthDate as birth_date,
FROM
   source
