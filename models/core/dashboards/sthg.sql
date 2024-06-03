SELECT
    budget.local_page,
    budget.region_page,
    budget.date,
    budget.milestone_name,
    {% for target in targets %}
      SUM(
        daily_{{ target }}
      ) AS daily_{{ target }},
    {% endfor %}
  FROM
    {{ ref("facebook_budget") }}
    budget
  WHERE
    budget.date <= CURRENT_DATE()
    {%- if is_incremental() %}
    and budget.date >= date_add(current_date, interval -3 day)
    {% else %}
    and budget.date >= '2023-11-01'
  {% endif -%}
  {{dbt_utils.group_by(4)}}