{{
  config(
    tags=['table','dimension']
    )
}}

WITH branches AS (
    SELECT
        *
    FROM
        {{ ref("stg_kiotviet__branches") }}
    WHERE 1=1
        {# branch_name LIKE '5S%' #}
        and branch_id not in (1000087891,86414)
),

asm_list as ( 
select 
    distinct
    *
from {{ ref("stg_gsheet__asms") }}
)

SELECT
    branches.branch_id,
    coalesce(branches.branch_name, asm_list.store_name) as branch_name,
    asm_list.branch_code,
    branches.channel,

    asm_list.asm_name,
    safe_cast(asm_list.phone as string) phone,
    asm_list.email,

    asm_list.local_page,
    asm_list.local_page_code,
    asm_list.region_page,
    asm_list.pic as fb_ads_pic,

    asm_list.province,
    asm_list.province_code,
    asm_list.region,
    asm_list.region_code,

    asm_list.opening_day,
    asm_list.close_date,

    asm_list.frontage,
    asm_list.area_sqm,
FROM
    asm_list
full outer JOIN  branches
    ON asm_list.store_name = branches.branch_name
union all 
select 
    channel_id as branch_id,
    channel as branch_name,
    cast(null as string) as branch_code,
    "Online & Ecom" as channel,

    "Online" as asm_name,
    cast(null as string) as phone,
    cast(null as string) as email,

    cast(null as string) as local_page,
    cast(null as string) as local_page_code,
    cast(null as string) as region_page,
    cast(null as string) as fb_ads_pic,

    "Online & Ecom" as province,
    "HNO" as province_code,
    "Online & Ecom" as region,
    "HNO" as region_code,

    cast(null as date) as opening_day,
    cast(null as date) as close_date,

    cast(null as float64) as frontage,
    cast(null as float64) as area_sqm,
    from {{ ref("stg_nhanhvn__sales_channels") }}