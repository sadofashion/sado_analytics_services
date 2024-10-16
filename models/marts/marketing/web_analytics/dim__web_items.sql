{{
  config(
    materialized = 'table',
    enabled = false
    )
}}

WITH products AS (
    SELECT
        product_name,
        product_code,
        categories.id AS category_id
    FROM
        {{ ref("stg_5sfashion__products") }}
        LEFT JOIN unnest(categories) categories
    WHERE
        product_code IS NOT NULL
),
categories AS (
    SELECT
        *
    FROM
        {{ ref("stg_5sfashion__categories") }}
        where  product_line not in ('BỘ SƯU TẬP')
)
SELECT
    product_code,
    product_name,
    coalesce(C.category,'(Chưa phân loại)') as category,
    coalesce(C.parent_category,'(Chưa phân loại)') as parent_category,
    coalesce(C.sub_productline, '(Chưa phân loại)') as sub_productline,
    coalesce(C.product_line, '(Chưa phân loại)') as product_line,
FROM
    products p
    LEFT JOIN categories C
    ON p.category_id = C.category_id 
qualify ROW_NUMBER() over (
        PARTITION BY product_code
        ORDER BY
            C.product_line_depth DESC,
            C.sub_productline_depth DESC,
            C.depth DESC
    ) = 1
