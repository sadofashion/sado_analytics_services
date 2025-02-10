{{
  config(
    materialized = 'view',
    tags=['fact',"training"],
    )
}}

{%set targets = {
  "num_trainee": "DT.001",
  "avg_training_hr": "DT.002",
  "rating": "DT.003",
  "avg_training_cost": "DT.004"
}%}

with _metrics as (
    select
        date_trunc(start_date,month) as month,
        sum(training_cost) as total_training_cost,
        sum(hours) as total_hours,
        count(emp_code) as total_trainees,
        sum(training_cost)/count(emp_code) as avg_training_cost,
        sum(hours)/count(emp_code) as avg_training_hr,
        avg(rating_avg) as avg_rating,
        avg(test_score) as avg_test_score,
        avg(rating_content) as avg_rating_content,
        avg(rating_online) as avg_rating_online,
        avg(rating_host) as avg_rating_host,
        avg(rating_instructor) as avg_rating_instructor,
    from {{ ref("stg_gsheet__internal_training") }}
    group by 1
),

_target as (
  select *
    from (
      select 
        reporting_period, 
        target_id, 
        value
      from {{ ref("stg_gsheet__company_targets") }}
      where department = 'P. Đào tạo'
  ) 
  pivot(
      sum(value) as target for target_id in (
        {% for k, v in targets.items() %}
          "{{ v }}" as {{ k }}{% if not loop.last %},{% endif %}
        {% endfor %}
      )
    )
)

select 
  m.*,
  t.* except(reporting_period)
from _metrics m 
left join _target t on m.month = t.reporting_period