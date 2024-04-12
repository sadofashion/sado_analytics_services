{{
  config(
    materialized = 'table',
    )
}}

SELECT
    client_key,
FROM
    {{ ref("dim_ga4__client_keys") }}
WHERE
    regexp_contains(LOWER(NAME), r'test|it|tesst')