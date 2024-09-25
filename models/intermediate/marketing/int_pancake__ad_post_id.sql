select
    split(ad_post_id)[offset(0)] as ad_page_id,
    ad_id, 
    {# {{dbt_utils.dbt_utils.generate_surrogate_key('ad_post_id', 'ad_id')}} as ad_post_id_surrogate_key #}
from {{ ref("base_pancake__conversations") }}
where ad_id is not null
and ad_post_id is not null
group by 1,2