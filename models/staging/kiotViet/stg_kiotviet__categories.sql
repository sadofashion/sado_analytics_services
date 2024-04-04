{{
  config(
    tags=['view', 'dimension','kiotviet']
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

SELECT
    r1.categoryName category,
    r1.categoryId as category_id,
    coalesce(r2.categoryName,r1.categoryName) AS sub_productline,
    coalesce(r3.categoryName,r2.categoryName,r1.categoryName) AS productLine,
    CASE
    {% for key,values in mapping.items() %}
      WHEN LOWER(r2.categoryName) IN ('{{ values|join("','") }}') or LOWER(r1.categoryName) IN ('{{ values|join("','") }}') THEN '{{key}}'
    {% endfor %}
    ELSE LOWER(
      r2.categoryName
    )
  END AS ads_product_mapping,
  CASE
    WHEN regexp_contains(
      lower(r3.categoryName),
      r'thu đông'
    ) THEN 'Hàng đông'
    WHEN regexp_contains(
      lower(r3.categoryName),
      r'xuân hè'
    ) THEN 'Hàng hè'
    ELSE 'Quanh năm'
  END AS product_group,
FROM
    {{ ref('base_kiotViet__categories') }}
    r1
    left JOIN {{ ref('base_kiotViet__categories') }}
    r2
    ON r1.parentId = r2.categoryId
    left JOIN {{ ref('base_kiotViet__categories') }}
    r3
    ON r2.parentId = r3.categoryId