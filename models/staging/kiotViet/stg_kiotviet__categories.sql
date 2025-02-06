{{
  config(
    tags=['view', 'dimension','kiotviet']
  )
}}

{% set mapping = { 
  "Quà tặng" : {
    "QTA":['^quà tặng',"nl qùa tặng túi xách balo"],
  },
  "Phụ kiện" : {
    "PKN":['tất','ba lỗ lót','sịp','phụ kiện',"áo lót cộc tay", "dây lưng","giày","khẩu trang","ví","carvat"],
  },
  "Quanh năm": {
    "SMD":['sơ mi dài'],
    "QKD":['quần khaki'],
    "QBD":['quần jean'],
    "QAU":["quần âu"],
  },
  "Thu đông": {
    "AKB":['áo bomber'],
    "ABZ":["áo blazer"],
    "AGB":["áo gió bộ"],
    "AKG":["áo gió","bộ gió","áo khoác gió","áo khoác"],
    "AGN":["áo giữ nhiệt"],
    "AKD":["áo khoác da"],
    "ALO":["áo len"],
    "ANO":["áo nỉ rời"],
    "AKC":["áo phao"],
    "APD":['áo polo dài tay'],
    "APO":["áo thun dài tay"],
    "ANB":["áo nỉ bộ"],
    "AVB":["áo vest","bộ đồ","bộ vest"],
    "BNI":["bộ nỉ",],
    "QGB":["quần gió bộ"],
    "QDC":["quần dài casual"],
    "QNB":["quần nỉ bộ"],
    "QVE":["quần vest"],
    "QDT":['quần dài thể thao'],
    "QNI":['quần nỉ rời',"quần nỉ casual"],
  },
  "Xuân hè" : {
    "ABL":['áo ba lỗ'],
    "ACN":['áo chống nắng'],
    "ASN": ['áo sát nách',],
    "ATS":['bộ t-shirt','tshirt',"t-shirt cũ","t-shirt thể thao","t-shirt thiết kế"],
    "QSG":['quần short gió'],
    "QSK":['quần short kaki'],
    "QST":['quần short thể thao cạp chun','quần short thể thao cạp cúc',"quần bộ polo","quần bộ tshirt","quần short vải","quần short âu"],
    "QSC":["quần short casual"],
    "SMC":['sơ mi cộc'],
    "APB":["áo bộ polo","bộ polo"],
    "APC":['polo'],
  },
  
} %}

WITH source AS (

    {{ dbt_utils.deduplicate(relation = source(
            'kiotViet',
            'p_categories_list'
        ), partition_by = 'categoryId', order_by = "modifiedDate DESC,_batched_at desc",) }}
)

{# with preprocess as ( #}
  SELECT
    r1.categoryName category,
    r1.categoryId as category_id,
    case when coalesce(r2.categoryName,r1.categoryName) = coalesce(r3.categoryName,r2.categoryName,r1.categoryName) then r1.categoryName else coalesce(r2.categoryName,r1.categoryName) end as sub_productline,
    {# coalesce(r2.categoryName,r1.categoryName) AS sub_productline, #}
    coalesce(r3.categoryName,r2.categoryName,r1.categoryName) AS productLine,
    CASE
    {% for season,product_groups in mapping.items() -%}
      {% for key,values in product_groups.items() -%}
      WHEN regexp_contains(LOWER(r2.categoryName),r'{{ values|join("|") }}') 
      or regexp_contains(LOWER(r1.categoryName),r'{{ values|join("|") }}') THEN '{{key|lower()}}'
      {% endfor -%}
    {% endfor -%}
    ELSE LOWER(r2.categoryName)
  END AS ads_product_mapping,
  CASE
    {% for season,product_groups in mapping.items() -%}
      {% for key,values in product_groups.items() -%}
    WHEN regexp_contains(lower(r1.categoryName),r'{{ values|join("|") }}') THEN '{{season|upper()}}'
      {% endfor -%}
    {% endfor -%}
    ELSE 'KHÔNG PHÂN LOẠI' END AS product_group,
FROM
    source
    r1
    left JOIN source
    r2
    ON r1.parentId = r2.categoryId
    left JOIN source
    r3
    ON r2.parentId = r3.categoryId
    {# )

  select 
  from preprocess p1 #}