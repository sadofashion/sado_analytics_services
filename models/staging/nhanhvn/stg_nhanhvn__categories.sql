{{
  config(
    tags=['view', 'dimension','nhanhvn']
  )
}}

with source as (
  {{ dbt_utils.deduplicate(
    relation = source(
        'nhanhvn',
        'p_categories'
    ),
    partition_by = 'id',
    order_by = "_batched_at desc",
) }}
)

SELECT
    c1.code AS category_code_level1,
    c1.id AS category_id_level1,
    c1.name AS category_name_level1,
    c2.code AS category_code_level2,
    c2.id AS category_id_level2,
    c2.name AS category_name_level2,
    c3.code AS category_code_level3,
    c3.id AS category_id_level3,
    c3.name AS category_name_level3,
    c4.code AS category_code_level4,
    c4.id AS category_id_level4,
    c4.name AS category_name_level4,
FROM
    source
    c1
    LEFT JOIN source
    c2
    ON c1.id = c2.parentId
    LEFT JOIN source
    c3
    ON c2.id = c3.parentId
    LEFT JOIN source
    c4
    ON c3.id = c4.parentId
WHERE
    c1.parentId IS NULL
