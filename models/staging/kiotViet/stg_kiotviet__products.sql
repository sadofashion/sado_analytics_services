{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='product_id',
    on_schema_change='sync_all_columns',
    tags = ['incremental', 'dimension','kiotviet']
) }}


WITH source AS (
    select * from {{source(
            'kiotViet',
            'p_products_list'
        )}}
      {% if is_incremental() %}
        where parse_date('%Y%m%d', _TABLE_SUFFIX) >= date_add(current_date, interval -1 day)
      {% endif %}
        
),

deduplicate as (
  {{ dbt_utils.deduplicate(
        relation = 'source',
        partition_by = 'id',
        order_by = "modifiedDate DESC,_batched_at desc",
    ) }}
)

SELECT
  p.id AS product_id,
  p.categoryId AS category_id,
  p.fullName AS product_name,
  REGEXP_REPLACE(p.fullName,r'\s\-.*$','') AS class_name,
  p.code AS product_code,
  REGEXP_REPLACE(p.code,r'\d{2}$','') AS class_code,
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
  deduplicate p
  INNER JOIN {{ ref('stg_kiotviet__categories') }} AS C
  ON p.categoryId = C.category_id
