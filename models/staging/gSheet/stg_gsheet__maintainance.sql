{{
  config(
    materialized = 'table',
    tags = ['table', 'fact','daily','dimension']
    )
}}

{%set default_duration = 7%}

SELECT
    date(created_at) created_at,
    branch_code,
    requested_category AS request_category,
    request_sub_category,
    description,
    related_documents,
    priority,
    maintainance_approval,
    execution_plan,
    safe_cast(replace(estimated_cost,",","") AS int64) AS estimated_cost,
    pic,
    coalesce(DURATION, {{default_duration}}) as duration,
    coalesce(deadline, DATE_ADD(created_at, INTERVAL {{default_duration}} DAY)) AS deadline,
    actual_finish_date,
    case 
        when  actual_finish_date is null then "Đang thực hiện"
        when coalesce(actual_finish_date, CURRENT_DATE()) > deadline then "Trễ Deadline" 
        when actual_finish_date <= deadline then "Đúng Deadline"
    end as status,
    safe_cast(replace(actual_cost,",","") AS int64) AS actual_cost,
    acceptance_date,
    acceptance_state,
    requester_type,
    date_diff(actual_finish_date,created_at,day) as duration_to_finish,
FROM
    {{ source('gSheet','maintainance_sheet') }}
WHERE
    created_at >= '2024-01-01'
