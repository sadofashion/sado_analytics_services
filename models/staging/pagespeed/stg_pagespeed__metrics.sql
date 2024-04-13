{{
  config(
    materialized = 'view',
    tags = ['pagespeed','fact','view']
    )
}}

{% set metrics = [ 
    'CUMULATIVE_LAYOUT_SHIFT_SCORE', 
    'EXPERIMENTAL_TIME_TO_FIRST_BYTE', 
    'FIRST_CONTENTFUL_PAINT_MS', 
    'FIRST_INPUT_DELAY_MS', 
    'INTERACTION_TO_NEXT_PAINT', 
    'LARGEST_CONTENTFUL_PAINT_MS' 
    ] %}

{% set categories = [ 
    'accessibility', 
    'best-practices', 
    'performance', 
    'pwa', 
    'seo' 
    ] %}

WITH source AS (
    SELECT
        url,
        strategy,
        DATE(_batched_at) AS analysis_date,
        JSON_VALUE(data,'$.loadingExperience.overall_category') AS overall_speed_category,
        {%for category in categories%}
        cast(json_value(data,'$.lighthouseResult.categories.{{category}}.score') as float64) AS {{category | replace('-','_')}}_score,
        {%endfor%}
        {% for metric in metrics %}
        json_extract(data,'$.loadingExperience.metrics.{{metric}}') AS {{ metric }},
        {% endfor %}
    FROM
        ({{ dbt_utils.deduplicate(
            relation = source('pagespeed','pagespeed'),
            partition_by = 'url,strategy,date(_batched_at)',
            order_by = '_batched_at desc') 
            }})
),
preprocess AS (
    SELECT
        *
    FROM
        source unpivot(
            metrics FOR metric_name IN ({{ metrics | join(',') }})
            )
)
SELECT
    * EXCEPT(distributions,metrics),
    json_value(metrics,'$.category') metric_category,
    cast(json_value(metrics,'$.percentile') as float64) metric_percentile_value,
    json_value(distributions,'$.min') || COALESCE('-' || json_value(distributions, '$.max'), '+') AS metric_range,
    cast(json_value(distributions,'$.proportion') as float64) AS range_proportion,
    CASE
        WHEN CAST(json_value(distributions, '$.min') AS int64) = 0 THEN 'FAST'
        WHEN json_value(distributions,'$.max') IS NULL THEN 'SLOW'
        ELSE 'AVERAGE'
    END AS range_distribution_category
FROM
    preprocess,
    unnest(json_extract_array(metrics, '$.distributions')) distributions
