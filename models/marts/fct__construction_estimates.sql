{{
  config(
    materialized = 'view',
    tags = ['fact','view']
    )
}}

with estimates as (
    select distinct
        c.project_id,
        c.branch_name,
        c.asm,
        c.created_at,
        c.province,
        setup_cost.*,
    from {{ ref("stg_gsheet__construction") }} c
    left join unnest(c.setup_cost) setup_cost
)

select 
    e.* ,
    b.area_sqm,
    b.frontage,
from estimates e
left join {{ ref("dim__branches") }} b on e.branch_name = b.branch_name