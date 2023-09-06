SELECT
    p.id as product_id,
    p.categoryId as category_id,
    p.fullName AS product_name,
    REGEXP_REPLACE(
        p.fullName,
        r'\s\-.*$',
        ''
    ) AS class_name,
    p.code as product_code,
    REGEXP_REPLACE(
        p.code,
        r'\d{2}$',
        ''
    ) AS class_code,
    p.trademarkName as trademarkName,
    p.isActive as is_active,
    p.type,
    p.attributes,
    C.sub_productline,
    C.category,
    C.productline
FROM
    {{ ref('base_kiotViet__products') }}
    p
    INNER JOIN {{ ref('stg_kiotviet__categories') }} AS C
    ON p.categoryId = C.category_id
