{{
  config(
    materialized = 'table',
    tags=['fact', 'gSheet',"company_targets","daily"],
    )
}}

select 
department,
date_trunc(parse_date('%m/%e/%Y', reporting_period),month) reporting_period,
target_id,
target_name,
unit,
value,
from {{ source('gSheet', 'target_2025') }}
where target_id is not null