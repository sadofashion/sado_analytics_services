{{
  config(
    tags=['fact', 'gSheet',"training"],
  )
}}

with source as (
  {{dbt_utils.deduplicate(
    relation= source('gSheet', 'internal_training'),
    partition_by = "emp_code,instructor,start_date,course_name",
    order_by = "_batched_at desc"
    )
  }}
)

select 
    emp_code,
    emp_name,
    format as course_format,
    course_name,
    instructor,
    date(date_add(start_date,interval 7 hour)) start_date,
    date(date_add(end_date,interval 7 hour)) end_date,
    safe_cast(hours as float64) hours,
    safe_cast(rating_content as float64) rating_content,
    safe_cast(rating_online as float64) rating_online,
    safe_cast(rating_host as float64) rating_host,
    safe_cast(rating_instructor as float64) rating_instructor,
    safe_cast(rating_avg as float64) rating_avg,
    safe_cast(test_score as float64) test_score,
    ifnull(safe_cast(training_cost as float64),0) training_cost,
from source
where emp_code is not null