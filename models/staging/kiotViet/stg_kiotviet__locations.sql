{{
  config(
    tags=['view', 'dimension','kiotviet']
  )
}}

WITH source AS (
        {{ dbt_utils.deduplicate(
        relation = source(
            'kiotViet',
            'p_locations_list'
        ),
        partition_by = 'id',
        order_by = "_batched_at desc",
    ) }}
)



SELECT
    NAME,
    normalName,
    regexp_extract(
        NAME,
        r'^(.*)\s-\s(?:.*)'
    ) AS province,
    regexp_extract(
        NAME,
        r'^(?:.*)\s-\s(.*)'
    ) AS district,
FROM
    source
