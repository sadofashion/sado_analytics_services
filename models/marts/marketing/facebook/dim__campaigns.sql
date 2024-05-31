{{
  config(
    materialized = 'table',
    tags = ['fb','fact','table']
    )
}}

with store_group as (
    select
    distinct 
    local_page, 
    local_page_code,
    region_code,
    province_code,
    from {{ ref('dim__branches') }}
)


select 
    c.* except(ad_location),
    case when c.ad_location_layer ="Country" then "Vietnam" else coalesce(s.local_page,p.province, r.region, c.ad_location) end as ad_location,
    coalesce(p.province,p2.province) as province,
    coalesce(r.region,r2.region) as region,
    "Vietnam" as country,
from {{ ref('stg_facebookads__campaigns') }} c
left join store_group s on case when c.convention_version_number = 'B2406' then c.ad_location = s.local_page else c.ad_location = s.local_page_code end 
left join {{ ref("dim__provinces") }} p on c.ad_location = p.province_code
left join {{ ref("dim__regions") }} r on c.ad_location = r.region_code
left join {{ ref("dim__provinces") }} p2 on s.province_code = p2.province_code
left join {{ ref("dim__regions") }} r2 on s.region_code = r2.region_code