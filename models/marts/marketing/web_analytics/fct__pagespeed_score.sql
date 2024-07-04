{{
    config(
        materialized = 'table',
        tags = ['pagespeed','fact','table']
    )
}}

{%set score_categories = ["accessibility_score","best_practices_score","performance_score","pwa_score","seo_score"]%}

select 
distinct
analysis_date,
url,
strategy,
-- metrics
overall_speed_category,
{{score_categories| join(',')}}, 
{%- set _ = score_categories.remove("pwa_score") %}
({{score_categories | join('+')}})/{{score_categories|length}} as overall_score
from {{ ref('stg_pagespeed__metrics') }}