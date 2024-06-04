{{
  config(
    materialized = 'table',
    tags = ['fb','fact','table']
    )
}}

{%- set extract_product -%}
    regexp_replace(regexp_extract(p.content_edge,r'\w{2} (.*)'),r" \d+","")
    {# regexp_extract(p.content_edge,r'\w{2} (.*)(?:\d{2})') #}
{%-endset-%}

with store_group as (
    select
    distinct 
    local_page, 
    local_page_code,
    region_code,
    province_code,
    from {{ ref('dim__branches') }}
),
product_group as (
    select distinct 
    category_name,
    lower(product_group_code) product_group_code,
    product_group,
    season,
    from 
    {{ ref("dim__product_categories") }}
),
preprocessing as 
(
    select 
        distinct c.* except(ad_location,event_name),
        case 
            when regexp_contains(content_edge,r"^th|sp th") then "Tổng hợp"
            when content_edge like "sp%" then "Sản phẩm"
            when content_edge like "cb%" then "Combo"
            when content_edge like "br%" then "Thương hiệu"
            when content_edge like "nd%" then "Nhận diện"
            when content_edge like "qt%" then "Quà tặng"
        end as content_edge_group,
        case when event_name = 'KT' then 'khai trương' else event_name end as event_name,
        case 
            when c.ad_location_layer ="Country" then "Vietnam" 
            else coalesce(s.local_page_code,p.province_code, r.region_code, b.branch_code,c.ad_location) 
        end as ad_location,
        case 
            when c.ad_location_layer ="Store" then coalesce(s.local_page_code, b.local_page_code) 
        end as ad_group_location,
        coalesce(p.province_code,p2.province_code,p3.province_code) as province,
        coalesce(r.region_code,r2.region_code, r3.region_code) as region,
        "Vietnam" as country,

from {{ ref('stg_facebookads__campaigns') }} c
-- join store group base on convention version number
left join store_group s on case when c.convention_version_number = 'B2406' then c.ad_location = s.local_page else c.ad_location = s.local_page_code end 
-- if the store group is null, join with branches
left join {{ ref("dim__branches") }} b on c.ad_location = b.branch_code and s.local_page_code is null
-- if the store group is null, join with branches
left join {{ ref("dim__provinces") }} p on c.ad_location = p.province_code
left join {{ ref("dim__provinces") }} p2 on s.province_code = p2.province_code
left join {{ ref("dim__provinces") }} p3 on b.province_code = p3.province_code

left join {{ ref("dim__regions") }} r on c.ad_location = r.region_code
left join {{ ref("dim__regions") }} r2 on s.region_code = r2.region_code
left join {{ ref("dim__regions") }} r3 on b.region_code = r3.region_code
)
select 
    p.* except(ad_pic),
    case 
        when ad_location in ('5SFTHA') or regexp_contains(lower(ad_pic),r'thang|thắng')  then 'Thắng'
        when ad_location in ('5SFTIE','TIEN') or regexp_contains(lower(ad_pic),r'tien|tiến') then 'Tiến'
        when ad_location in ('5SFTUN','TUNG') or regexp_contains(lower(ad_pic),r'tung|tùng')  then 'Tùng'
        when ad_location in ('5SFTRA') or regexp_contains(lower(ad_pic),r'trang|ht') then 'Trang'
        when ad_location in ('5SFTUY','TUYE') or  regexp_contains(lower(ad_pic),r'tuyen|tuyền') then 'Tuyền'
    else ad_pic end as ad_pic,
    upper(coalesce(pg.category_name,{{extract_product}}) ) as category_name,
    upper(coalesce(pg.product_group,{{extract_product}}) ) as product_group,
    upper(coalesce(pg.season,{{extract_product}})) as season,
    from preprocessing p
    left join product_group pg on regexp_extract(p.content_edge,r'\w{2} (\w{3})') = pg.product_group_code

