{{
  config(
    materialized = 'view',
    tags = ['fact','view']
    )
}}

with raw_ as (
    select 
        c.project_id,
        c.branch_name,
        c.asm,
        c.created_at,
        c.province,
        c.construction_type,
        c.project_id||":pw:"||paper_works.type as task_id,
        "2.Hoàn thiện hồ sơ quyết toán" as task_group_name,
        paper_works.type as task_name,
        paper_works.deadline,
        paper_works.finish_date,
        1 as step_num,
        case
            when coalesce(paper_works.finish_date,current_date) > paper_works.deadline then "Trễ Deadline"
            when paper_works.finish_date = paper_works.deadline then "Đúng Deadline"
            when paper_works.finish_date < paper_works.deadline then "Trước Deadline"
        end as flag
    from {{ ref("stg_gsheet__construction") }} c
    left join unnest(c.paper_works) paper_works

    union all 

    select 
        c.project_id,
        c.branch_name,
        c.asm,
        c.created_at,
        c.province,
        c.construction_type,
        c.project_id||":pc:"||procedure.type as task_id,
        "1.Quy trình thi công" as task_group_name,
        procedure.type as task_name,
        procedure.deadline,
        procedure.finish_date,
        procedure.step_num,
        case
            when coalesce(procedure.finish_date,current_date) > procedure.deadline then "Trễ Deadline"
            when procedure.finish_date = procedure.deadline then "Đúng Deadline"
            when procedure.finish_date < procedure.deadline then "Trước Deadline"
        end as flag
    from {{ ref("stg_gsheet__construction") }} c
    left join unnest(c.procedure) procedure
)
select *,
lag(finish_date) over (partition by project_id,task_group_name order by step_num) as previous_task_finish_date
from raw_