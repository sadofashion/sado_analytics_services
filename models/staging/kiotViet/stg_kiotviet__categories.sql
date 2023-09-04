SELECT
    r1.categoryName category,
    r1.categoryId as category_id,
    r2.categoryName AS sub_productline,
    r3.categoryName AS productLine
FROM
    {{ ref('base_kiotViet__categories') }}
    r1
    INNER JOIN {{ ref('base_kiotViet__categories') }}
    r2
    ON r1.parentId = r2.categoryId
    INNER JOIN {{ ref('base_kiotViet__categories') }}
    r3
    ON r2.parentId = r3.categoryId
