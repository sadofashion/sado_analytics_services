{{ config(
  materialized = 'table',
  partition_by ={ 'field': 'date',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'merge',
  unique_key = ['date','branch_id'],
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'fact','dashboard']
) }}

{% set targets = [ "sales_target", "traffic_target"] %}

{% set rev_calcols ={ "transaction_id" :"count(distinct ",
"customer_id":"count(distinct ",
"total" :"sum(",
"total_payment" :"sum(" } %}

{% set rev_types = ["invoice", "return"] %}

WITH offline_performance AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['r.branch_id', 'date(r.transaction_date)']) }} AS branch_working_day_id,
    {% for col,
      cal in rev_calcols.items() %}
      {{ cal + " " + col + ")" }} AS {{ "val_" + col }},
      {% for type in rev_types -%}
        {{ cal }}
        CASE
          WHEN transaction_type = '{{type}}' THEN {{ col }}
        END {{ ")" }} AS {{ "num_" + type + "_" + col }},
      {% endfor %}
    {% endfor %}
    COUNT(DISTINCT r.branch_id) AS num_stores,
    count(distinct case when r.transaction_type = 'invoice' and r.transaction_code not like '%HDD%' and r.total>0 then r.transaction_id end) as num_invoices,
    sum(case when r.transaction_type = 'invoice' and r.transaction_code not like '%HDD%' then r.total end) as invoice_value,
  FROM
    {{ ref("fct__transactions") }} r
  WHERE 1=1
    {% if is_incremental() %}
      and DATE(r.transaction_date) >= date_add(CURRENT_DATE,INTERVAL -7 DAY) 
    {% else %}
      and r.transaction_date >= '2023-01-01'
    {% endif %}
    AND r.branch_id NOT IN (1000087891) 
GROUP BY 1
),

budget AS (
  SELECT
  budget.* except(
  {% for item in targets -%}
      {{ "daily_"+item }} {{ ", " if not loop.last }}
  {% endfor -%}
  ),
  {{ dbt_utils.generate_surrogate_key(['budget.branch_id', 'budget.date']) }} AS branch_working_day_id,
  {%for t in targets -%}
  coalesce({{"fbudget.daily_"+t}},{{"budget.daily_"+t}}) as {{"daily_"+t}},
  {% endfor -%}
  FROM
    {{ ref("fct__sales_budget") }} budget
    full outer join {{ref("facebook_budget")}} fbudget on budget.branch_id = fbudget.branch_id and budget.date = fbudget.date
  WHERE
    budget.date <= CURRENT_DATE()
  {% if is_incremental() %}
    AND budget.date >= date_add(CURRENT_DATE,INTERVAL -7 DAY) 
  {% endif %}
),

_traffic AS (
  SELECT
    branch_name,
    DATE,
    SUM(traffic) AS total_traffic,
    SUM(working_hour) AS total_working_hour,
    from {{ ref('stg_gsheet__traffic') }}
    group by 1,2
),

_offline_performance2 as (
  select 
    {{ dbt_utils.generate_surrogate_key(['r.branch_id', 'date(r.transaction_date)']) }} AS branch_working_day_id,
    SUM(CASE WHEN (r.subtotal) <> 0  THEN (r.quantity) ELSE NULL END) as units_sold,
    SUM(r.cogs) as total_cogs,
    sum(r.order_discount) order_discount,
  from {{ref("fct__revenue_items")}} r
  WHERE 1=1
    {% if is_incremental() %}
      and DATE(r.transaction_date) >= date_add(CURRENT_DATE,INTERVAL -7 DAY) 
    {% else %}
      and r.transaction_date >= '2023-01-01'
    {% endif %}
    AND r.branch_id NOT IN (1000087891) 
  GROUP BY 1
)

SELECT
  b.* EXCEPT(branch_working_day_id,branch_name),
  o.* EXCEPT(branch_working_day_id),
  t.* except(branch_name, date),
  o2.* except(branch_working_day_id)
FROM budget b
left outer join offline_performance o on b.branch_working_day_id = o.branch_working_day_id
left outer join _offline_performance2 o2 on b.branch_working_day_id = o2.branch_working_day_id
left outer join _traffic t on b.branch_name = t.branch_name and b.date = t.date

