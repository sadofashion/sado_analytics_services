{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'date',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'insert_overwrite',
  unique_key = ['date','page'],
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
      {# fb.pic, #}
    {% for metric in metrics %}
    sum(fb.{{metric}}) as {{metric}},
    {% endfor %}
    from {{ref("facebook_performance")}} fb
    where date_start >= '2023-11-01'
    and (
      fb.page in (select distinct a.new_ads_page from {{ref("dim__offline_stores")}} a)
      or
      fb.page in (select distinct a.old_ads_page from {{ref("dim__offline_stores")}} a)
      or 
      fb.page in ("5SFTHA","5SFTIE","5SFTUN","5SFTRA","5SFT","5SFG","5SF")
      )
    group by 1,2
),
facebook_budget as (
  select 
  budget.page,
  budget.date,
  budget.milestone_name,
  {# budget.pic, #}
  {% for target in targets %}
    sum(daily_{{target}}) as daily_{{target}},
    {% endfor %}
    from {{ref("facebook_budget")}} budget
    where budget.date <=current_date()
    group by 1,2,3
),
offline_performance as (
  select 
  a.new_ads_page as page,
  a.new_ads_pic as pic,
  date(r.transaction_date) transaction_date,
  {% for col, cal in rev_calcols.items() %}
    {{cal}} {{col}}{{")"}} as val_{{col}},
    {% for type in rev_types%}
      {{cal}} case when transaction_type = '{{type}}' then {{col}} end{{")"}} as num_{{type}}_{{col}}, 
    {% endfor %}
  {% endfor %}
  count(distinct r.branch_id) as num_stores,
  from {{ref("revenue")}} r
  inner join {{ref("dim__offline_stores")}} a 
  on r.branch_id = a.branch_id
  where r.transaction_date >='2023-11-01'
  and r.branch_id not in (1000087891)
  group by 1,2,3
),

asms as (
  select distinct a.asm_name,
  a.new_ads_page as page,
  a.new_ads_pic as pic,
  a.old_ads_page as old_page,
  a.old_ads_pic as old_pic,
  from {{ref("dim__offline_stores")}} a
)

SELECT
distinct
p.* except(page,date_start),
o.* except(page,transaction_date,pic),
b.* EXCEPT(date, page, milestone_name),
coalesce(p.date_start,o.transaction_date,b.date) as date,
coalesce(p.page,o.page,b.page) as page,
asms.asm_name,
coalesce(asms.pic,o.pic) as pic,
from facebook_performance p
full join facebook_budget b on p.date_start = b.date and (p.page = b.page)
full join offline_performance o on  o.transaction_date = p.date_start and (o.page = p.page)
left join asms on coalesce(p.page,o.page,b.page) = asms.page

