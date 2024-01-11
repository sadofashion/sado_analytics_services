{{ config(
    materialized = 'incremental',
    partition_by ={ 'field': 'date',
    'data_type': 'date',
    'granularity': 'month' },
    unique_key = ['branch_id','date'],
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'fact','budget','ignore']
) }}

{% set targets = ["budget", "sales_target", "traffic_target",'aov'] %}
WITH processed AS (

    SELECT
        *
    EXCEPT
        ({% for item in targets %}
            val_{{ item }} {{ ", " if not loop.last }}
        {% endfor %}),
        {% for item in targets %}
            safe_divide(val_{{ item }}, date_diff(tb.END, tb.start, DAY) + 1) AS daily_{{ item }},
        {% endfor %}
FROM
    (
        SELECT
            branch_id,
            branch,
            {# budget_month, #}
            b.page,
            milestones.*,
            b.pic,
            {# {% for item in targets %}
            {{ item }} AS total_ {{ item }},
        {% endfor %}

        #}
        FROM
            {{ ref('stg_gsheet__facebook_budget') }} b,
            unnest(milestones) milestones
    ) 
    pivot (
    SUM(VALUE) AS val for budget_type IN (
        {% for item in targets %}
            '{{item}}' {{ ", " if not loop.last }}
        {% endfor %}
    )
    ) AS tb
)
SELECT
    processed.*,
    C.date,
FROM
    processed
    CROSS JOIN {{ ref("calendar") }} C 
WHERE
    C.date >= processed.start
    AND C.date <= processed.end {# {{dbt_utils.group_by(14)}} #}
        {% if is_incremental() %}
          and processed.start >=  date_add(date(_dbt_max_partition))
        {% endif %}