{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = 'product_id',
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'dimension','kiotviet']
) }}

{% set patterns ={ 
  "tax_code":{ 
    "custom":{ '22': '^[X0]{1,2}',
             '23': '^[Y0]{1,2}' },
    "default": '^Y{1,2}|^0{1,2}',},
  "product_design_code":{ "custom":{"24":'^(?:[ZXY0]{0,3})([BC0][A-Z]{3}[0-9]{3,5})'}, "default": '^(?:[ZXY0]{0,2})([BC0]?[A-Z]{3}[0-9]{3,5})',},
  "color_code":{ "default": '^(?:[ZXY0]{0,3})(?:[BC0]?[A-Z]{3}[0-9]{3,5})([A-Z]{3})',},
  "size_code":{ "default": '^(?:[ZXY0]{0,3})(?:[BC0]?[A-Z]{3}[0-9]{3,5})(?:[A-Z]{3})(\d{2})',},
  "weaving_method_code":{ "default": '^[ZXY0]{0,2}?(?:[CB0]?[A-Z]{3}\d{3,5})(?:[A-Z]{3})(?:\d{2})([KT0])' },
  "form_code":{ "default": "^[ZXY0]{0,2}?(?:[CB0]?[A-Z]{3}\d{3,5})(?:[A-Z]{3})(?:\d{2})(?:[KT0])([0-6])" }
}%}

WITH source AS (

  SELECT
    *
  FROM
    {{ source(
      'kiotViet',
      'p_products_list'
    ) }}

  {% if is_incremental() %}
  WHERE
    parse_date('%Y%m%d',_TABLE_SUFFIX) >= date_add(CURRENT_DATE,INTERVAL -1 DAY)
  {% endif %}
),
deduplicate AS (
  {{ dbt_utils.deduplicate(
    relation = 'source',
    partition_by = 'id',
    order_by = "modifiedDate DESC,_batched_at desc",
  ) }}
),
preprocess AS (
  SELECT
    p.id AS product_id,
    p.categoryId AS category_id,
    p.fullName AS product_name,
    REGEXP_REPLACE(p.fullName,r'\s\-.*$','') AS class_name,
    regexp_extract(p.code,r'-?(\w+)$') AS product_code,
    regexp_extract(p.code,r'(\w+)-') AS product_code_prefix,
    COALESCE(regexp_extract(p.code, r'[A-Z]{3}(2[1-5])'), "Cũ") AS year,
    REGEXP_REPLACE(p.code,r'\d{2}$','') AS class_code,
    p.trademarkName AS trademarkName,
    p.allowsSale AS is_alow_sale,
    p.isActive AS is_active,
    CASE
      p.type
      WHEN 1 THEN 'Combo'
      WHEN 2 THEN 'Hàng hoá'
      WHEN 3 THEN 'Dịch vụ'
    END AS type,
    p.attributes,
    C.sub_productline,
    C.category,
    C.productline,
    C.ads_product_mapping,
    C.product_group,
  FROM
    deduplicate p
    INNER JOIN {{ ref('stg_kiotviet__categories') }} AS C
    ON p.categoryId = C.category_id
)
SELECT
  *,
  coalesce(safe_cast(regexp_extract(product_code_prefix,r'[CX]?(\d+)[CX]?') as int64),1) as product_multiplier,
  {% for field, config in patterns.items() -%}
    {% if config.custom is defined -%}
    CASE
        {% for key, pattern in config.custom.items()-%}
        WHEN p.year = "{{key}}" THEN regexp_extract(product_code, r"{{ pattern }}")
        {% endfor -%}
      else regexp_extract(product_code, r"{{ config['default'] }}") end
      {% else -%}
      regexp_extract(product_code, r"{{ config['default'] }}")
    {%- endif -%}  AS {{ field }},
  {% endfor -%}
FROM
  preprocess p
