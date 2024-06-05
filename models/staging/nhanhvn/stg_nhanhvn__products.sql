{{ config(
  tags = ['view', 'dimension','nhanhvn']
) }}

{% set productline_mapping ={ "BỘ ĐỒ - THU ĐÔNG" :["Bộ gió","Bộ Vest","Bộ nỉ"],
"BỘ ĐỒ - XUÂN HÈ" :["Bộ T-shirt","Bộ Polo"],
"QUẦN - QUANH NĂM" :["Quần âu","Quần khaki","Quần Jean"],
"QUẦN - THU ĐÔNG" :["Quần dài thể thao","Quần nỉ rời"],
"QUẦN - THU ĐÔNG" :["Quần nỉ rời","Quần short gió"],
"QUẦN - XUÂN HÈ" :["Quần short gió","Quần short thể thao cạp chun","Quần short kaki","Quần short thể thao cạp cúc","Quần short casual"],
"ÁO - QUANH NĂM" :["Sơ Mi Dài"],
"ÁO - THU ĐÔNG" :["Áo giữ nhiệt","Áo polo dài tay","Áo Blazer","Áo thun dài tay","Áo Bomber","Áo nỉ rời","Áo len","Áo phao","Áo gió"],
"ÁO - XUÂN HÈ" :["Tshirt","Áo sát nách","Polo","Áo ba lỗ","Áo chống nắng","Sơ mi cộc"],
"ĐỒ LÓT, PHỤ KIỆN" :["TẤT","BA LỖ LÓT","PHỤ KIỆN","SỊP"] } %}

{% set ads_product_mapping ={ 
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


{% set sub_productline_patterns ={ "Bộ gió" :["bộ gió"],
"Bộ Vest" :["bộ vest","vest"],
"Bộ nỉ" :["bộ nỉ"],
"Bộ T-shirt" :["bộ t-shirt",'áo phông'],
"Bộ Polo" :["bộ polo"],
"Quần âu" :["quần âu"],
"Quần khaki" :["quần khaki","kaki"],
"Quần Jean" :["quần jean"],
"Quần dài thể thao" :["quần dài thể thao","jogger",'quần gió'],
"Quần nỉ rời" :["quần nỉ"],
"Quần short gió" :["quần short gió"],
"Quần short thể thao cạp cúc" :["quần short thể thao cạp cúc"],
"Quần short thể thao cạp chun" :["quần short thể thao cạp chun","quần short thể thao"],
"Quần short kaki" :["quần short kaki","quần short khaki"],
"Quần short casual" :["quần short casual"],
"Sơ Mi Dài" :["sơ mi dài"],
"Áo giữ nhiệt" :["áo giữ nhiệt"],
"Áo polo dài tay" :["polo dài tay"],
"Áo Blazer" :["blazer"],
"Áo thun dài tay" :["áo thun"],
"Áo Bomber" :["bomber"],
"Áo nỉ rời" :["áo nỉ"],
"Áo len" :["áo len"],
"Áo phao" :["áo phao","áo khoác chần bông","áo khoác lông vũ","áo khoác siêu nhẹ","áo khoác phao"],
"Áo gió" :["áo gió","áo khoác gió"],
"Tshirt" :["tshirt","t-shirt"],
"Áo sát nách" :["áo sát nách","tanktop","tank top"],
"Polo" :["polo"],
"Áo ba lỗ" :["áo ba lỗ"],
"Áo chống nắng" :["áo chống nắng"],
"Sơ mi cộc" :["sơ mi cộc"],
"TẤT" :["tất"],
"BA LỖ LÓT" :["ba lỗ"],
"PHỤ KIỆN" :["phụ kiện","ví"],
"SỊP" :["sịp","quần lót"] } %}



WITH source AS (
  {{ dbt_utils.deduplicate(
    relation = source(
      'nhanhvn',
      'p_products'
    ),
    partition_by = 'idNhanh',
    order_by = "_batched_at desc",
  ) }}
),
category AS (
  SELECT
    safe_cast(products.idNhanh AS int64) AS product_id,
    products.typeName AS type_name,
    safe_cast(products.avgCost AS int64) AS avg_cost,
    products.code AS product_code,
    COALESCE(regexp_extract(products.code,r'([A-Za-z0-9-]+)(?:[A-Z]{3}\d{1,5}$)'),products.code) AS class_code,
    products.barcode,
    products.name AS product_name,
    safe_cast(products.price AS int64) price,
    products.status AS product_status,
    categories.name AS category_name,
    CASE
      {% for sub_productline,pattern in sub_productline_patterns.items() -%}
        WHEN regexp_contains(LOWER(products.name), r'{{pattern|join("|")}}') THEN '{{sub_productline}}'
      {% endfor -%}
    END AS sub_productline
  FROM
    source products
    LEFT JOIN {{ ref('base_nhanhvn__categories') }}
    categories
    ON products.categoryId = categories.id
)
SELECT
  *,
  CASE
    {% for product,values in ads_product_mapping.items() -%}
      WHEN LOWER(sub_productline) IN ('{{values|join("','")}}') THEN '{{product | lower()}}'
    {% endfor -%}
  END AS ads_product_mapping,
  CASE
    {% for product,values in productline_mapping.items() -%}
      WHEN sub_productline IN ('{{values|join("','")}}') THEN '{{product}}'
    {% endfor -%}
  END AS productline,
FROM
  category
