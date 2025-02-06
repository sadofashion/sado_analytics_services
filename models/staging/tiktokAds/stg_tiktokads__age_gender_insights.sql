{{ config(
    tags = ['view','tiktok']
) }}

{% set dimensions = ["ad_id", "age", "gender", "stat_time_day"] %}
{% set metrics ={ "string" :["adgroup_id", "adgroup_name", "advertiser_id", "advertiser_name","campaign_id", "campaign_name","placement_type","objective_type",],
"float64" :["average_video_play", "clicks", "comments", "conversion", "conversion_rate", "cost_per_conversion", "cost_per_result", "cpc", "cpm", "ctr", "follows", "impressions", "likes", "profile_visits", "real_time_conversion", "result", "result_rate", "shares", "spend", "video_views_p100", "video_views_p25", "video_views_p50", "video_views_p75", "video_watched_2s", "video_watched_6s"] } %}
WITH source AS (
    {{ dbt_utils.deduplicate(
        relation = source(
            'tiktok',
            'ad_age_gender'
        ),
        partition_by = "JSON_VALUE(data, '$.dimensions.ad_id'),JSON_VALUE(data, '$.dimensions.stat_time_day'), JSON_VALUE(data, '$.dimensions.age'), JSON_VALUE(data, '$.dimensions.gender')",
        order_by = '_batched_at desc'
    ) }}
),
unnested_keys AS (
    SELECT
        {% for dimension in dimensions -%}
            json_value(
                DATA,
                '$.dimensions.{{dimension}}'
            ) AS {{ dimension }},
        {%- endfor %}

        {%- for type,
            metric_group in metrics.items() -%}
            {%- for metric in metric_group -%}
                safe_cast(json_value(DATA, '$.metrics.{{metric}}') AS {{ type }}) AS {{ metric }},
            {% endfor %}
        {%- endfor %}
    FROM
        source
)
SELECT
    *
EXCEPT(stat_time_day),
    DATE(stat_time_day) AS date
FROM
    unnested_keys
