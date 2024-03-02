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
phone, email
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
FROM
    asm_list
    LEFT JOIN  branches
    ON asm_list.store_name = branches.branch_name

{# 
select
distinct
branches.*,
v.* except(branch_id)
from branches 
left join (select 
new_values.*, 
coalesce(old_values.old_ads_page, new_values.new_ads_page) old_ads_page, 
coalesce(old_values.old_ads_pic, new_values.new_ads_pic) old_ads_pic, 
from new_values
left join old_values on old_values.branch_id = new_values.branch_id) v on branches.branch_id = v.branch_id #}