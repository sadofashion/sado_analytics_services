{{
  config(
    materialized = 'view',
    tags=['view','dimension']
    )
}}
select 
distinct
class_code,
first_value(sub_productline ignore nulls) over w1 as sub_productline,
first_value(productline ignore nulls) over w1 as productline,
first_value(category ignore nulls) over w1 as category,
first_value(product_group ignore nulls) over w1 as product_group,
first_value(season ignore nulls) over w1 as season
from {{ ref("dim__inventory_items") }}
where class_code is not null
and (category not in ('WEBSITE (Không xóa)') or category is null)
window w1 as (partition by class_code order by kiotviet_product_id rows BETWEEN unbounded preceding and unbounded following)
{# and kiotviet_product_id is not null #}