{{
  config(
    tags=['view', 'dimension','nhanhvn']
  )
}}

SELECT
    safe_cast(products.idNhanh as int64) AS product_id,
    products.typeName AS type_name,
    safe_cast(products.avgCost as int64) AS avg_cost,
    products.code as product_code,
    coalesce(regexp_extract(products.code,r'([A-Za-z0-9-]+)(?:[A-Z]{3}\d{1,5}$)'),products.code) as class_code,
    products.barcode,
    products.name as product_name,
    safe_cast(products.price as int64) price,
    products.status as product_status,
    categories.name as category_name
    -- coalesce(categories.category_name_level4,categories.category_name_level3,categories.category_name_level2,categories.category_name_level1) as category_name,
    -- coalesce(categories.category_name_level3,categories.category_name_level2,categories.category_name_level1) as product_type,
    -- coalesce(categories.category_name_level2,categories.category_name_level1) as sub_productline,
    -- coalesce(categories.category_name_level1) as productline,
FROM
    {{ ref('base_nhanhvn__products') }} products
    left join {{ref('base_nhanhvn__categories')}} categories 
    on products.categoryId = categories.id
    -- coalesce(categories.category_id_level4,categories.category_id_level3,categories.category_id_level2,categories.category_id_level1)
    -- where coalesce(categories.category_name_level1) is not null