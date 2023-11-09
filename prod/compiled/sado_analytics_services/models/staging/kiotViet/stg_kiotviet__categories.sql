

SELECT
    r1.categoryName category,
    r1.categoryId as category_id,
    r2.categoryName AS sub_productline,
    r3.categoryName AS productLine
FROM
    `agile-scheme-394814`.`dbt_dev`.`base_kiotViet__categories`
    r1
    INNER JOIN `agile-scheme-394814`.`dbt_dev`.`base_kiotViet__categories`
    r2
    ON r1.parentId = r2.categoryId
    INNER JOIN `agile-scheme-394814`.`dbt_dev`.`base_kiotViet__categories`
    r3
    ON r2.parentId = r3.categoryId