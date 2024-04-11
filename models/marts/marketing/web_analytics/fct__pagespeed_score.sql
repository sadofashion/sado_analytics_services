{{
    config(
        materialized = 'table',
        tags = ['pagespeed','fact','table']
    )
}}

select 
distinct
analysis_date,
url,
strategy,
-- metrics
overall_speed_category,
accessibility_score,
best_practices_score,
performance_score,
pwa_score,
seo_score
from {{ ref('stg_pagespeed__metrics') }}