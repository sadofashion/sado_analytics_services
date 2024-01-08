{{ config(
    tags = ['table', 'fact','budget','ignore']
) }}

{% set targets = ["budget", "sales_target", "traffic_target",'aov'] %}
WITH processed AS (

    SELECT
        *
    EXCEPT
        ({% for item in targets %}
            val_{{ item }} {{ ", " if not loop.last}}
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
            {{ ref('stg_gsheet__facebook_budget') }}
            b,
            unnest(milestones) milestones
    ) pivot (SUM(VALUE) AS val for budget_type IN (
        {% for item in targets %}
        '{{item}}' {{ ", " if not loop.last}}
    {% endfor %}
    )) AS tb
)
SELECT
    processed.*,
    C.date,
    {# {% for metric in metrics %%}
    SUM(
        fb.{{ metric }}
    ) {{ metric }},
{% endfor % %}
#}
FROM
    processed
    CROSS JOIN {{ ref("calendar") }} C {# left join {{ref("facebook_performance")}} fp on processed.page = fp.page and c.date = fp.date_start #}
WHERE
    C.date >= processed.start
    AND C.date <= processed.
END {# {{dbt_utils.group_by(14)}} #}
