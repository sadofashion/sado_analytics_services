{{
  config(
    tags=['view', 'dimension','nhanhvn']
  )
}}


SELECT
    safe_cast(id as int64) AS user_id,
    userName AS user_name,
    email AS email,
    mobile AS contact_number,
    roleName AS role,
FROM
    {{ ref('base_nhanhvn__users') }}
