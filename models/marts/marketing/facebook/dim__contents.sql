{{
  config(
    materialized = 'table',
    tags = ['fb','dimensions','table']
    )
}}

select 
* except (sheet_name,_batched_at),
case when sheet_name = 'PAGE TỔNG' then 'social' else 'performance' end as content_type,
from {{ ref("stg_gsheet__contents") }}