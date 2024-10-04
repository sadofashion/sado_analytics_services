{{
  config(
    materialized = 'view',
    tags = ['fact','view']
    )
}}

select 
    c.project_id,
    c.branch_name,
    c.asm,
    c.created_at,
    c.province,
    setup_cost.*,
    b.area_sqm,
    b.frontage,
from {{ ref("stg_gsheet__construction") }} c
left join unnest(c.setup_cost) setup_cost
left join {{ ref("dim__branches") }} b on c.branch_name = b.branch_name