{% set items = {
    "light":['áo ba lỗ','áo chống nắng','áo sát nách','polo','sơ mi cộc','tshirt','bộ polo','bộ t-shirt','quần short âu','quần short casual','quần short gió','quần short kaki','quần short thể thao cạp chun','quần short thể thao cạp cúc','áo giữ nhiệt','áo polo dài tay','áo thun dài tay'],
    "heavy":['áo blazer','áo bomber','áo phao','bộ vest'],
    "medium": ['áo gió','áo khoác','áo len','áo nỉ rời','bộ gió','bộ nỉ','quần dài casual','quần dài thể thao','quần nỉ rời','quần âu','quần jean','quần khaki','sơ mi dài'],
    "accessories":['áo lót cộc tay','ba lỗ lót','dây lưng','sịp','tất','ví','phụ kiện']
}
%}

{% set conditions = {
    "has_items": {
        "num_light_items+num_heavy_items+num_medium_items+num_accessories_items": ">=1",
    },
    "accessory_only": {
        "num_accessories_items": ">0",
        "num_light_items": "= 0",
        "num_heavy_items": "= 0",
        "num_medium_items": "= 0"
    },
    "single_medium": {
        "num_medium_items": "= 1",
        "num_heavy_items": "= 0",
        "num_light_items": "= 0",
        "num_accessories_items": "= 0"
    },
    "single_light_with_accessory": {
        "num_light_items": "= 1",
        "num_accessories_items": ">=0",
        "num_heavy_items": "= 0",
        "num_medium_items": "= 0"
    },
    "two_light_without_accessory": {
        "num_light_items": "= 2",
        "num_accessories_items": "= 0",
        "num_heavy_items": "= 0",
        "num_medium_items": "= 0"
    },
    "single_medium_with_accessory": {
        "num_medium_items": "= 1",
        "num_accessories_items": ">=1",
        "num_heavy_items": "= 0",
        "num_light_items": "= 0"
    },
    "light_mix_medium_with_accessory_B": {
        "num_light_items+num_medium_items": ">=3",
        "num_medium_items+num_light_items": "< 8",
        "num_accessories_items": ">0",
        "num_heavy_items": "= 0"
    },
    "single_heavy_with_accessory": {
        "num_heavy_items": "= 1",
        "num_accessories_items": ">=0",
        "num_light_items": "= 0",
        "num_medium_items": "= 0"
    },
    "light_mix_medium_with_accessory_A": {
        "num_light_items+num_medium_items": ">=8",
        "num_medium_items+num_light_items": "<= 12",
        "num_accessories_items": ">= 0",
        "num_heavy_items": "= 0"
    },
    "two_heavy_with_accessory": {
        "num_heavy_items": "= 2",
        "num_accessories_items": ">=0",
        "num_light_items": "= 0",
        "num_medium_items": "= 0"
    },
    "single_heavy_mix_with_accessory": {
        "num_heavy_items": "= 1",
        "num_accessories_items": ">= 0",
        "num_light_items+num_medium_items": ">=1",
    },
    "two_heavy_mix_with_accessory": {
        "num_heavy_items": "= 2",
        "num_accessories_items": ">= 0",
        "num_light_items+num_medium_items": ">=1",
    },
}
%}


with agg_ as (
    select 
date_trunc(r.transaction_date, month) as month,
regexp_extract(transaction_code,r'([A-Z0-9]+)\.') as original_code,
{% for key,values in items.items() %}
ifnull(sum(case when lower(it.sub_productline) in ('{{values|join("','")}}') then r.quantity end),0) as {{'num_'+key+'_items'}},
{%endfor%}
from {{ ref('fct__revenue_items') }} r
left join {{ref("stg_kiotviet__products")}} it on r.product_id = it.product_id
left join {{ref('dim__branches')}} b on r.branch_id = b.branch_id
where 1=1
and r.transaction_code not like '%HDD%'
and r.transaction_type = 'invoice'
and r.source ='kiotviet'
and r.transaction_date >='2023-12-01'
and b.channel in ('Offline','Popup','Điểm xả')
{{dbt_utils.group_by(2)}}
)
select 
month,
count(distinct original_code) as num_orders,
{% for field,conds_ in conditions.items() -%}
count(distinct 
case when  
{% for cons, value in conds_.items() -%}
    {{cons}} {{value}} {{'and' if not loop.last }}
{% endfor -%}
then original_code end ) as {{field}},
{% endfor -%}
from agg_
group by 1
having has_items*2 - ({{' + '.join(conditions.keys())}}) > 0