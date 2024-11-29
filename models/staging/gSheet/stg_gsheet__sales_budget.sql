{{ config(
    materialized = 'table',
    tags = ['dimension','table', 'ignore']
) }}


{% set targets ={ 
    "sales_target" :"doanh số",
    "sales_target_psd" :"dt/ ngày",
    "sales_per_visit":"gttb/kh",
    "traffic":"lượt khách",
    "gross_margin":"blg",
    "upt":"upt",
    "ausp":"gía bán tb",
    "aov" :'aov' 
    } %}

WITH source AS (

    SELECT
        *
    EXCEPT(`ASM - Showroom`,`Mô hình`)
    FROM
        {{ source('gSheet','sales_budget') }}
    WHERE
        MONTH >= '2023-11-01' 
    qualify ROW_NUMBER() over ( PARTITION BY showroom, MONTH ORDER BY _batched_at DESC) = 1
),
formated AS (
    SELECT
        CASE
            WHEN source.showroom = '5S Hải Dương' THEN '5S Hải Dương 1'
            WHEN source.showroom = '5S Thái Nguyên 1' THEN '5S Thái Nguyên'
            WHEN source.showroom = '5S GO' THEN '5S Go Thái Bình'
            WHEN source.showroom = '5S Hà Tĩnh 1' THEN '5S Hà Tĩnh'
            WHEN source.showroom = '5S Thái Bình 1' THEN '5S Thái bình 1'
            ELSE source.showroom
        END AS branch,
        source.month AS budget_month,
        {# source.target, #}
        ARRAY_AGG(
            STRUCT(
                milestones.start,
                milestones.end,
                milestones.value,
                regexp_extract(milestones.key,r'^(.*) -') AS milestone_name,
                CASE
                    LOWER(regexp_extract(milestones.key, r'- ([^()]+)$')) 
                    {% for key,value in targets.items() -%}
                        WHEN '{{ value }}' THEN '{{ key }}'
                    {% endfor -%}
                END AS budget_type
            )
        ) AS milestones
    FROM
        source,
        unnest(milestones) milestones 
        {{ dbt_utils.group_by(2) }}
)
SELECT
    formated.*,
    {# branch.branch_id, #}
    {# branch.local_page, #}
    {# branch.region_page #}
    {# asm.new_ads_page as page,
    asm.new_ads_pic AS pic,
    #}
FROM
    formated {# LEFT JOIN {{ ref('dim__offline_stores') }} #}
    {# branch #}
    {# ON lower(formated.branch) = lower(branch.branch_name) #}
