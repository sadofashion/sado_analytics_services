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
{# {%for cat in score_categories%}
{{cat}},
{%endfor%} #}
{{score_categories| join(',')}}, 
{# ({%for cat in score_categories%} {{cat}} {{'+' if not loop.last}} {%endfor%} #}
({{score_categories | join('+')}})/5 as overall_score
from {{ ref('stg_pagespeed__metrics') }}