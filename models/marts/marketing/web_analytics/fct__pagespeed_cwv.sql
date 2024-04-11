{{
  config(
    materialized = 'table',
    tags = ['pagespeed','fact','table']
    )
}}

{%set metrics = ['CUMULATIVE_LAYOUT_SHIFT_SCORE', 'EXPERIMENTAL_TIME_TO_FIRST_BYTE', 'FIRST_CONTENTFUL_PAINT_MS', 'FIRST_INPUT_DELAY_MS', 'INTERACTION_TO_NEXT_PAINT', 'LARGEST_CONTENTFUL_PAINT_MS']%}

select 
distinct
analysis_date,
url,
strategy,
metric_range,
range_distribution_category,
{%for metric in metrics%}
avg(percentile_{{ metric | lower() }}) as percentile_{{ metric | lower() }},
avg(proportion_{{ metric | lower() }}) as proportion_{{ metric | lower() }},
{%endfor%}

from {{ ref('stg_pagespeed__metrics') }}
PIVOT (avg(metric_percentile_value) as percentile, avg(range_proportion) as proportion FOR metric_name IN (
    {%for metric in metrics%} 
    '{{ metric }}' as  {{ metric | lower() }} {{ ',' if not loop.last }}
    {% endfor %}
))

{{dbt_utils.group_by(5)}}