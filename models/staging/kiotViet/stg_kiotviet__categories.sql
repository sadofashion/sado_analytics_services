{{
  config(
    tags=['view', 'dimension','kiotviet']
  )
}}

{% set mapping = { 
  "ABZ":["áo blazer"],
  "ACN":['áo chống nắng'],
  "AGB":["áo gió bộ"],
  "AGN":["áo giữ nhiệt"],
  "AKB":['áo bomber'],
  "AKC":["áo phao"],
  "AKD":["áo khoác da"],
  "AKG":["áo gió","bộ gió"],
  "ALO":["áo len"],
  "ANB":["áo nỉ bộ"],
  "ANO":["áo nỉ rời"],
  "APB":["áo bộ polo"],
  "APC":['bộ polo','polo'],
  "APD":['áo polo dài tay'],
  "APO":["áo thun dài tay"],
  "ATS":['bộ t-shirt','tshirt',"t-shirt cũ","t-shirt thể thao","t-shirt thiết kế","áo sát nách"],
  "ATT":['áo sát nách','áo ba lỗ'],
  "AVB":["áo vest","bộ đồ","bộ vest"],
  "BNI":["bộ nỉ",],
  "PKN":['tất','ba lỗ lót','sịp','phụ kiện'],
  "QAU":["quần âu"],
  "QBD":['quần jean'],
  "QDT":['quần dài thể thao'],
  "QGB":["quần gió bộ"],
  "QKD":['quần khaki'],
  "QNB":["quần nỉ bộ"],
  "QNI":['quần nỉ rời'],
  "QSG":['quần short gió'],
  "QSK":['quần short kaki'],
  "QST":['quần short thể thao cạp chun','quần short thể thao cạp cúc',"quần bộ polo","quần bộ tshirt","quần short vải","quần short âu"],
  "QSC":["quần short casual"],
  "QVE":["quần vest"],
  "SMC":['sơ mi cộc'],
  "SMD":['sơ mi dài'],
} %}


{# with preprocess as ( #}
  SELECT
    r1.categoryName category,
    r1.categoryId as category_id,
    coalesce(r2.categoryName,r1.categoryName) AS sub_productline,
    coalesce(r3.categoryName,r2.categoryName,r1.categoryName) AS productLine,
    CASE
    {% for key,values in mapping.items() -%}
      WHEN LOWER(r2.categoryName) IN ('{{ values|join("','") }}') or LOWER(r1.categoryName) IN ('{{ values|join("','") }}') THEN '{{key|lower()}}'
    {% endfor -%}
    ELSE LOWER(
      r2.categoryName
    )
  END AS ads_product_mapping,
  CASE
    WHEN regexp_contains(lower(r3.categoryName),r'thu đông') THEN 'THU ĐÔNG'
    WHEN regexp_contains(lower(r3.categoryName),r'xuân hè') THEN 'XUÂN HÈ'
    WHEN regexp_contains(lower(r3.categoryName),r'xuân hè') THEN 'XUÂN HÈ'
    ELSE 'QUANH NĂM'
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
    {# )

  select 
  from preprocess p1 #}