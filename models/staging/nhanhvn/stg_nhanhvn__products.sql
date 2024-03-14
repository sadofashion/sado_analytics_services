{{
  config(
    tags=['view', 'dimension','nhanhvn']
  )
}}

{% set mapping ={ "áo nỉ" :["áo nỉ rời"],
"áo thun" :["áo thun dài tay"],
"bomber" :['áo bomber'],
"polo dài tay" :['áo polo dài tay'],
"quần gió " :['quần dài thể thao'],
"quần nỉ" :['quần nỉ rời'],
"polo" :['bộ polo','polo'],
"short gió" :['quần short gió'],
"short kk" :['quần short kaki'],
"short tt" :['quần short thể thao cạp chun','quần short thể thao cạp cúc'],
"smc" :['sơ mi cộc'],
"t-shirt" :['bộ t-shirt','tshirt'],
"tanktop" :['áo sát nách','áo ba lỗ'],
"jeans" :['quần jean'],
"kaki dài" :['quần khaki'],
"phụ kiện" :['tất','ba lỗ lót','sịp','phụ kiện'],
"smd" :['sơ mi dài'],} %}


WITH source AS (
    {{ dbt_utils.deduplicate(relation = source(
            'nhanhvn',
            'p_products'
        ), partition_by = 'idNhanh', order_by = "_batched_at desc",) }}
)

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
FROM
    source products
    left join {{ref('base_nhanhvn__categories')}} categories 
    on products.categoryId = categories.id