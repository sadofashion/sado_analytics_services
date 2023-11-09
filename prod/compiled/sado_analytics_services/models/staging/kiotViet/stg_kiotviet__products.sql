

SELECT
    p.id AS product_id,
    p.categoryId AS category_id,
    p.fullName AS product_name,
    REGEXP_REPLACE(
        p.fullName,
        r'\s\-.*$',
        ''
    ) AS class_name,
    p.code AS product_code,
    REGEXP_REPLACE(
        p.code,
        r'\d{2}$',
        ''
    ) AS class_code,
    p.trademarkName AS trademarkName,
    p.isActive AS is_active,
    p.type,
    p.attributes,
    C.sub_productline,
    C.category,
    C.productline
FROM
    `agile-scheme-394814`.`dbt_dev`.`base_kiotViet__products`
    p
    INNER JOIN `agile-scheme-394814`.`dbt_dev`.`stg_kiotviet__categories` AS C
    ON p.categoryId = C.category_id