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
    WHERE
        branch_name LIKE '5S%'
        and branch_id not in (1000087891)
),

asm_list as ( 
select 
    distinct
    *
from {{ ref("stg_gsheet__asms") }}
)

SELECT
    branches.branch_id,
    branches.branch_name,
    asm_list.branch_code,
    case when asm_list.asm_name is null then 'Kho & CH khác Kiotviet' else 'Offline' end as channel,

    asm_list.asm_name,
    asm_list.phone,
    asm_list.email,

    asm_list.local_page,
    asm_list.region_page,
    asm_list.pic as fb_ads_pic,

    asm_list.province,
    asm_list.province_code,
    asm_list.region,
    asm_list.region_code,

    asm_list.opening_day,
    asm_list.close_date
FROM
    asm_list
    LEFT JOIN  branches
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
    cast(null as string) as region_page,
    cast(null as string) as fb_ads_pic,

    "Online & Ecom" as province,
    "HNO" as province_code,
    "Online & Ecom" as region,
    "HNO" as region_code,

    cast(null as date) as opening_day,
    cast(null as date) as close_date,
    from {{ ref("stg_nhanhvn__sales_channels") }}