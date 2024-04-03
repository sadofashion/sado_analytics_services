{{
  config(
    tags=['table','dimension','sdc']
    )
}}

WITH branches AS (
    SELECT
        *
    FROM
        {{ ref("stg_kiotviet__branches") }}
    WHERE
        branch_name LIKE '5S%'
        and branch_id not in (1000087891)
),

asm_list as ( 
select 
    distinct
    asm_name,
    store_name,
    local_page,
    region_page,
    pic as fb_ads_pic,
    phone, 
    email,
    province,
    region,
from {{ ref("stg_gsheet__asms") }}
)

SELECT
    asm_list.asm_name,
    branches.branch_id,
    branches.branch_name,
    asm_list.phone,
    asm_list.email,
    asm_list.local_page,
    asm_list.region_page,
    asm_list.fb_ads_pic,
    asm_list.province,
    asm_list.region,
FROM
    asm_list
    LEFT JOIN  branches
    ON asm_list.store_name = branches.branch_name