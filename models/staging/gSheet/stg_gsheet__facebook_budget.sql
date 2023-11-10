{{ config(
    materialized = 'table',
    tags = ['dimension','table', 'ignore']
) }}
{%- set relation = source('gSheet','_ext_facebook_budget') -%}

{% set all_columns = adapter.get_columns_in_relation(relation) %}
{% set targets = { "budget":"ngân sách",
"sales_target":"doanh số",
"traffic_target":"lượt khách" } %}

WITH source AS (

    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY showroom,
            MONTH
            ORDER BY
                _batched_at DESC
        ) rn_
    FROM
        {{ source(
            'gSheet',
            '_ext_facebook_budget'
        ) }}
    WHERE
        MONTH >= '2023-11-01'
)

SELECT
    source.showroom AS branch,
    source.month AS budget_month,
    {% for col in all_columns %}
        {% for key,value in targets.items()  %}
            {% if value in col.name.lower() %}
                source.`{{col.name}}` AS {{key}},
            {% endif %}
        {% endfor %}
    {% endfor %}
    ARRAY_AGG(
        STRUCT(
            milestones.start,
            milestones.
        END,
        milestones.value,
        regexp_extract(
            milestones.key,
            r'^(.*) -'
        ) AS milestone_name,
        CASE
            LOWER(regexp_extract(milestones.key, r'- ([^()]+)$')) 
            {% for key, value in targets.items() %}
                WHEN '{{ value }}' THEN '{{ key }}'
            {% endfor %}
        END AS budget_type
    )
) as milestones
FROM
   source,
    unnest(milestones) milestones
WHERE
    rn_ = 1
{{dbt_utils.group_by(5)}}