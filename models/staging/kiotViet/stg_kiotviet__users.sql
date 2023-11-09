{{
  config(
    tags=['view', 'dimension','kiotviet']
  )
}}

SELECT
    id as user_id,
    userName as user_name,
    givenName as given_name,
    birthDate as birth_date,
FROM
    {{ ref('base_kiotViet__users') }}
