{{ config(
    materialized = 'incremental',
    partition_by ={ 'field': 'date',
    'data_type': 'date',
    'granularity': 'month' },
    unique_key = ['branch_id','date'],
    incremental_strategy = 'insert_overwrite',
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'fact','budget','ignore']
) }}

{% set sum_targets = ["sales_target", "traffic_target",] %}
{% set const_targets = ["gross_margin", "upt","ausp","aov","cr"] %}

{% set targets = sum_targets|list + const_targets|list %}

WITH processed AS (
    SELECT
        *
    EXCEPT
        ({% for item in targets -%}
            val_{{ item }} {{ ", " if not loop.last }}
        {% endfor -%}),
        {% for item in sum_targets -%}
            safe_divide(val_{{ item }}, date_diff(tb.end, tb.start, DAY) + 1) AS daily_{{ item }},
        {% endfor -%}
        {% for item in const_targets -%}
            val_{{ item }} {{ ", " if not loop.last }}
        {% endfor -%}
FROM
    (
        SELECT
            case when branch =  '5S Nam Giang' then '5S Nam Giang 2' else branch end as branch,
            milestones.*,
        FROM
            {{ ref('stg_gsheet__sales_budget') }} b,
            unnest(milestones) milestones
    ) 
    pivot (
    SUM(VALUE) AS val for budget_type IN ('{{targets|join("','")}}')
    ) AS tb
)

select 
w.branch_id,
w.promotion,
w.date,
w.branch_name,
{% for t in sum_targets if t -%}
{{"daily_"+t}},
{% endfor -%}
{% for t in const_targets if t -%}
{{"val_"+t}},
{% endfor -%}
from {{ref("int__working_days")}} w
left join processed p on lower(p.branch) = lower(w.branch_name)
and p.start <= w.date and p.end >= w.date