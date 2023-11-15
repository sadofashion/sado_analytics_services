{{
  config(
    tags=['table', 'fact','budget']
  )
}}

{% set targets = ["budget", "sales_target", "traffic_target"] %}

with processed as (

SELECT
    * 
EXCEPT
    ({% for item in targets %}
        val_{{ item }}
        {{ ", " if not loop.last else "" }}
    {% endfor %}),

    {% for item in targets %}
        safe_divide(val_{{ item }}, date_diff(tb.end,tb.start, day)+1) AS daily_{{ item }},
    {% endfor %}
FROM
    (
        SELECT
            branch_id,
            branch,
            budget_month,
            b.page,
            milestones.*,
            {% for item in targets %}
                {{ item }} as total_{{ item }},
            {% endfor %}
        FROM
            {{ ref('stg_gsheet__facebook_budget') }} b,
            unnest(milestones) milestones
    )
    pivot (
        SUM(value) AS val 
        for budget_type IN (
            {% for item in targets %}
        '{{item}}' {{ ", " if not loop.last else "" }}
            {% endfor %})
            ) as tb
)

select 
    processed.*,
    c.date,
    {# {% for metric in metrics %%}
    sum(fb.{{metric}}) {{metric}},
    {% endfor %%} #}
from processed
cross join {{ref("calendar")}} c
{# left join {{ref("facebook_performance")}} fp on processed.page = fp.page and c.date = fp.date_start #}
where c.date >= processed.start and c.date <= processed.end
{# {{dbt_utils.group_by(14)}} #}