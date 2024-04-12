select 
product_code,

from {{ ref("stg_5sfashion__products") }} p
left join {{ ref("stg_5sfashion__categories") }} c on p.category_id = c.category_id