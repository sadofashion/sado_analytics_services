{%set timezone = 'Asia/Saigon'%}

{% set paper_work_steps = {
    "constructing":"Xây dựng",
    "electricity":"Hệ thống điện/ĐH",
    "signboard":"Biển bảng",
    "interior":"Nội thất",
    "others":"Khác",
} %}


{% set procedure_steps = {
    "site_review":"Khảo sát mặt bằng",
    "construction_estimate":"Dự toán & PA Thi công",
    "start_construction":"Bắt đầu thi công",
    "facility_transfer":"Bàn giao",
    "preliminary_acceptance_review":"Nghiệm thu lần 1",
    "defect_fix":"Sửa chữa lỗi (nếu có)",
    "final_acceptance_review":"Nghiệm thu cuối",
    "post_transfer_review":"Feedback sau bàn giao",
} %}

with source as (
    {{dbt_utils.deduplicate(
        relation = source('gSheet','constructing'),
        partition_by = "json_value(data,'$.row_number')",
        order_by = "_batched_at desc"
    )}}
)

SELECT
    {{dbt_utils.generate_surrogate_key(
        [
            "nullif(json_value(data,'$.row_number'),'')" , 
            "nullif(json_value(data,'$.branch_name'),'')"
            ]
        )}} as project_id,

    nullif(json_value(data,'$.branch_name'),'') as branch_name,
    nullif(json_value(data,'$.asm'),'') as asm,
    nullif(json_value(data,'$.created_at'),'') as created_at,
    nullif(json_value(data,'$.province'),'') as province,
    "Thi công" as construction_type,

    date(timestamp(nullif(json_value(data,'$.bd_info.review_start_date'),"")),'{{timezone}}') as review_start_date,
    date(timestamp(nullif(json_value(data,'$.bd_info.actual_start_contruction_date'),"")),'{{timezone}}') as actual_start_contruction_date,

    {# date(nullif(json_value(data,'$.bd_info.actual_start_contruction_date'),''),'{{timezone}}') as actual_start_contruction_date, #}
    array(
       select as struct
       case {% for k,v in paper_work_steps.items() %}
        when json_value(paper_works,'$.key') = '{{k}}' then '{{v}}'
       {%endfor%} end as type,
        date(timestamp(nullif(json_value(paper_works,'$.value.deadline'),"")),'{{timezone}}') as deadline,
        date(timestamp(nullif(json_value(paper_works,'$.value.finish_date'),"")),'{{timezone}}') as finish_date,
        json_value(paper_works,'$.value.step_flag') as step_flag,
    from unnest({{var("json_transform_schema")}}.json_transform(json_extract(data,'$.paper_works'))) as paper_works) as paper_works,
    array(
       select as struct
       case {% for k,v in procedure_steps.items() %}
        when json_value(procedure,'$.key') = '{{k}}' then '{{v}}'
       {%endfor%} end as type,
        date(timestamp(nullif(json_value(procedure,'$.value.deadline'),"")),'{{timezone}}') as deadline,
        date(timestamp(nullif(json_value(procedure,'$.value.finish_date'),"")),'{{timezone}}') as finish_date,
        json_value(procedure,'$.value.step_flag') as step_flag,
        safe_cast(json_value(procedure,'$.value.step_num') as int64) as step_num,
    from unnest({{var("json_transform_schema")}}.json_transform(json_extract(data,'$.procedure'))) as procedure
    where json_value(procedure,'$.key') not in ('review_notes','constructing_notes')
    ) as procedure,
    array(
        select as struct
            case {% for k,v in paper_work_steps.items() %}
            when json_value(value_type,'$.key') = '{{k}}' then '{{v}}'
            {%endfor%} else json_value(value_type,'$.key') end as cost_category,
            sum(case when json_value(setup_cost,'$.key') ='actual' then safe_cast(json_value(value_type,'$.value') as int64) end)  as actual,
            sum(case when json_value(setup_cost,'$.key') ='estimate' then safe_cast(json_value(value_type,'$.value') as int64) end) as estimate
    from unnest({{var("json_transform_schema")}}.json_transform(json_extract(data,'$.setup_cost'))) as setup_cost,
    unnest({{var("json_transform_schema")}}.json_transform(json_extract(setup_cost,'$.value'))) as value_type
    where json_value(setup_cost,'$.key') not in ('description','flag','total_invest_ment')
    group by case {% for k,v in paper_work_steps.items() %}
            when json_value(value_type,'$.key') = '{{k}}' then '{{v}}'
            {%endfor%} else json_value(value_type,'$.key') end
    ) as setup_cost,

FROM
    {{ source('gSheet','constructing') }}
{# left join unnest({{var("json_transform_schema")}}.json_transform(json_extract(data,'$.paper_works'))) as paper_works #}
{# left join unnest({{var("json_transform_schema")}}.json_transform(json_extract(data,'$.procedure'))) as procedure #}
WHERE
    nullif(json_value(data,'$.row_num'),'') is not null
