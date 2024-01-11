{{
  config(
    tags=['table','dimension','sdc']
    )
}}
with facebook_regions as 
(
select distinct region
from {{ ref("stg_facebookads__regioninsights") }}
)