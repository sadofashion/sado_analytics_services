{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'transaction_date',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'insert_overwrite',
  unique_key = ['transaction_date','page'],
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'fact','dashboard']
) }}

{% set metrics = ["impressions","spend","clicks","reach","link_click","post_engagement","offline_conversion_purchase","offline_conversion_purchase_value","pixel_purchase","pixel_purchase_value","meta_purchase","meta_purchase_value","_results_message"] %}
{% set targets = ["budget", "sales_target", "traffic_target"] %}
{% set rev_calcols = {"transaction_id":"count(distinct", "total":"sum(", "total_payment":"sum("} %}
{% set rev_types = ["invoice", "return"] %}



with facebook_performance as (
    select 
      fb.page, date_start,
    {% for metric in metrics %}
    sum(fb.{{metric}}) as {{metric}},
    {% endfor %}
    from {{ref("facebook_performance")}} fb
    where date_start >= '2023-11-01'
    group by 1,2
),
facebook_budget as (
  select 
  budget.page,
  budget.date,
  budget.milestone_name,
  {% for target in targets %}
    sum(daily_{{target}}) as daily_{{target}},
    {% endfor %}
    from {{ref("facebook_budget")}} budget
    group by 1,2,3
),
offline_performance as (
  select 
  a.asm_name,
  a.page,
  date(r.transaction_date) transaction_date,
  {% for col, cal in rev_calcols.items() %}
    {{cal}} {{col}}{{")"}} as val_{{col}},
    {% for type in rev_types%}
      {{cal}} case when transaction_type = '{{type}}' then {{col}} end{{")"}} as num_{{type}}_{{col}}, 
    {% endfor %}
  {% endfor %}
  from {{ref("revenue")}} r
  inner join {{ref("stg_gsheet__asms")}} a on r.branch_id = a.branch_id
  where r.transaction_date >='2023-11-01'
  group by 1,2,3
)

SELECT
o.*,
b.* EXCEPT(date, page),
p.* except(page, date_start)
from offline_performance o
left join facebook_budget b on o.page = b.page and o.transaction_date = b.date
left join facebook_performance p on o.page = p.page and o.transaction_date = p.date_start