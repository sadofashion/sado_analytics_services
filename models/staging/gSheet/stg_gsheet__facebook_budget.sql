{{ config(
    materialized = 'table',
    tags = ['dimension','table', 'ignore']
) }}

{%- set relation = source(
    'gSheet',
    'facebook_budget'
) -%}
{% set all_columns = adapter.get_columns_in_relation(relation) %}
{% set targets ={ "budget" :"ngân sách",
"sales_target" :"doanh số",
"traffic_target" :"lượt khách",
"aov" :'gttb/kh' } %}
WITH source AS (

    SELECT
        * except(`ASM - Showroom`),
        ROW_NUMBER() over (
            PARTITION BY showroom,
            MONTH
            ORDER BY
                _batched_at DESC
        ) rn_
    FROM
        {{ source(
            'gSheet',
            'facebook_budget'
        ) }}
    WHERE
        MONTH >= '2023-11-01'
),
formated AS (
    SELECT
        CASE
            WHEN source.showroom = '5S Hải Dương' THEN '5S Hải Dương 1'
            WHEN source.showroom = '5S Thái Nguyên 1' THEN '5S Thái Nguyên'
            WHEN source.showroom = '5S GO' THEN '5S Go Thái Bình'
            WHEN source.showroom = '5S Hà Tĩnh 1' THEN '5S Hà Tĩnh'
            ELSE source.showroom
        END AS branch,
        source.month AS budget_month,
        {# source.target, #}
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
                {% for key,value in targets.items() %}
                    WHEN '{{ value }}' THEN '{{ key }}'
                {% endfor %}
            END AS budget_type
        )
) AS milestones
FROM
    source,
    unnest(milestones) milestones
WHERE
    rn_ = 1 {{ dbt_utils.group_by(2) }}
)
SELECT
    formated.*,
    branch.branch_id,
    asm.page,
    asm.pic,
FROM
    formated
    LEFT JOIN {{ ref('stg_kiotviet__branches') }}
    branch
    ON formated.branch = branch.branch_name
    LEFT JOIN {{ ref('stg_gsheet__asms') }}
    asm
    ON branch.branch_id = asm.branch_id
