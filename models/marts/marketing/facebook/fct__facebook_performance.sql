{{
  config(
    materialized = 'table',
    enabled = false,
    )
}}

{%set current_naming_convention = '2406'%}

select 
from {{ ref("stg_facebookads__adsinsights") }}