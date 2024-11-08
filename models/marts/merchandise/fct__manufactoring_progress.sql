{{
  config(
    materialized = 'view',
    tags = ['fact','view','daily']
    )
}}

{% set mapping = { 
  "Phụ kiện" : {
    "Phụ kiện":['0TAT',"0BLL",'0BOX','0BRF'],
  },
  "Quanh năm": {
    "Áo sơ mi (dài)": ["0ASM","0SMD"],
    "Quần Kaki": ["0QKK","0QKD"],
    "Quần Jean": ["0QJE","0QBD"],
    "Quần âu":["0QAU"],
  },
  "Thu đông": {
    "Áo Bomber":['0AKB'],
    "Áo Blazer":['0ABZ'],
    "Bộ gió": ["BAKG","BQDT","BAGB"],
    "Áo Gió (lẻ)":['0AKG'],
    "Áo giữ nhiệt":["0AGN"],
    "Áo khoác thời trang": ["0AKH"],
    "Áo khoác da":["0AKD"],
    "Áo len": ["0ALE"],
    "Áo nỉ (rời)":["0ANI","0ANO","0AKN"],
    "Áo phao": ["0APH","0AKC"],
    "Áo polo dài tay":['0APD'],
    "Áo thun dài tay":["0APO","0ATH"],
    "Bộ nỉ": ["BANI","BQNI","BAKN","BANK","BANH"],
    "Quần gió (lẻ)": ["0QDT"],
    "Quần nỉ (lẻ)":['0QNI'],
    "Quần dài Casual":["0QDC"],
  },
  "Xuân hè" : {
    "Áo ba lỗ": ["0ABL"],
    "Áo chống nắng": ["0ACN"],
    "Áo sát nách": ["0ASN"],
    "Áo T-shirt": ["0ATS"],
    "Quần short gió": ["0QSG"],
    "Quần short kaki": ["0QSK"],
    "Quần short thể thao": ["0QST"],
    "Quần short casual": ["0QSC"],
    "Sơ mi cộc": ["0SMC"],
    "Áo bộ polo": ["0APB"],
    "Áo Polo": ["0APC"],
  },
  
} %}

-- This model is used to store the manufacturing progress of the products.
WITH plan AS (
    SELECT
        product_code,
        original_code,
        {# po_code, #}
        sum(expected_deliver_amount) as expected_deliver_amount,
        max(expected_deliver_date) expected_deliver_date,
    FROM
        {{ ref("stg_gsheet__manufacture_plan") }}
        where product_code is not null
    group by 1,2
),
-- actual progress
actual_progress as (
    select 
    po.transaction_code,
    po.transaction_status,
    po.supplier_id,
    date(po.transaction_date) transaction_date,
    p.product_design_code,
    sum(quantity) quantity
    from {{ ref("stg_kiotviet__purchaseorderdetails") }} po
    left join {{ ref('stg_kiotviet__products') }} p on po.product_id = p.product_id
    where po.product_id is not null
    and po.branch_id = 49880
    and po.supplier_id not in (209080)
    and po.transaction_status not in ('Đã huỷ')
    {{dbt_utils.group_by(5)}}
)

SELECT
  plan.*,
  ap.transaction_code,
  ap.transaction_status,
  ap.supplier_id,
  ap.transaction_date,
  {# ap.product_design_code, #}
  ap.quantity as actual_receipt_amount,
  greatest(count(ap.transaction_code) over (partition by plan.product_code),1) as num_receipt_count,
  case 
  {% for seasonal,items in mapping.items()%}
    {% for key, values in items.items() -%}
      when left(plan.product_code,4) in ('{{values|join("','")}}') then '{{key}}' 
    {% endfor -%}
  {%endfor%} 
  end as product_group,
  case
  {% for seasonal,items in mapping.items() -%}
    {% for key, values in items.items() -%}
      when left(plan.product_code,4) in ('{{values|join("','")}}') then '{{seasonal}}' 
    {% endfor -%}
  {%endfor%} end as seasonal,

FROM
  plan
LEFT JOIN actual_progress ap on plan.product_code = ap.product_design_code
and date_trunc(plan.expected_deliver_date,month) = date_trunc(date(ap.transaction_date),month)
