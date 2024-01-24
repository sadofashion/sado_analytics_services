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

old_values as ( 
select 
distinct
branch_id,
asm_name,
fb_ads_page as old_ads_page,
fb_ads_pic as old_ads_pic,
from {{ ref("offline_ads_pages") }}
where dbt_valid_to is not null and branch_id is not null
qualify row_number() over (partition by branch_id order by dbt_valid_to desc) =1
),

new_values as ( 
select 
distinct
branch_id,
asm_name,
fb_ads_page as new_ads_page,
fb_ads_pic as new_ads_pic,
from {{ ref("offline_ads_pages") }}
where dbt_valid_to is null and branch_id is not null
)

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
left join old_values on old_values.branch_id = new_values.branch_id) v on branches.branch_id = v.branch_id