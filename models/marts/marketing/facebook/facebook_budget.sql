{% set targets = ["budget", "sales_target", "traffic_target"] %}
SELECT
    *
EXCEPT
    ({% for item in targets %}
        val_{{ item }}
        {{ ", " if not loop.last else "" }}
    {% endfor %}),
    {% for item in targets %}
        val_{{ item }} AS {{ item }},
    {% endfor %}
FROM
    (
        SELECT
            branch_id,
            branch,
            budget_month,
            milestones.*,
            {% for item in targets %}
                {{ item }} as total_{{ item }},
            {% endfor %}
        FROM
            {{ ref('stg_gsheet__facebook_budget') }},
            unnest(milestones) milestones
    ) 
    pivot (
        SUM(value) AS val 
        for budget_type IN (
            {% for item in targets %}
        '{{item}}' {{ ", " if not loop.last else "" }}
            {% endfor %})
            )
