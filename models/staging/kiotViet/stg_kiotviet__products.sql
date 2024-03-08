{{ config(
  tags = ['view', 'dimension','kiotviet']
) }}

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
  case p.type when 1 then 'Combo' when 2 then 'Hàng hoá' when 3 then 'Dịch vụ' end as type,
  p.attributes,
  C.sub_productline,
  C.category,
  C.productline,
  c.ads_product_mapping,
  C.product_group,
FROM
  {{ ref('base_kiotViet__products') }}
  p
  INNER JOIN {{ ref('stg_kiotviet__categories') }} AS C
  ON p.categoryId = C.category_id
