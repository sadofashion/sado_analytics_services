{{
  config(
    materialized = 'table',
    tags = [
      'staging',
      'gsheet',
      'fact',
      'daily'
    ]
    )
}}

select 
    parse_date('%e/%m/%Y', date) as date,
    branch_name,
    safe_cast(total_data as int) total_data,
    safe_cast(surveyed as int) total_survey,
    safe_cast(satisfied as int) satisfied,
    safe_cast(normal as int) neutral,
    safe_cast(unsatisfied as int) dissatisfied,
    (safe_cast(satisfied as int) - safe_cast(unsatisfied as int)) as net_gain,
from {{ source('gSheet', 'customer_survey') }}
where date is not null