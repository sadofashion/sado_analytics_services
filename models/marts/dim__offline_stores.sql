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
)

select 
branches.*,
a.asm_name,
a.fb_ads_page,
a.fb_ads_pic,
date(a.dbt_valid_from) dbt_valid_from,
date(a.dbt_valid_to) dbt_valid_to,
from branches
left join {{ ref("offline_ads_pages") }} a on a.branch_id = branches.branch_id