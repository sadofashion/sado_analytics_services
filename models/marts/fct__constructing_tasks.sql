select 
c.project_id,
c.branch_name,
c.asm,
c.created_at,
c.province,
paper_works.*
from {{ ref("stg_gsheet__construction") }} c
left join unnest(c.paper_works) paper_works