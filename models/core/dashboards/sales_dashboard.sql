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

{% set targets = ["budget", "sales_target", "traffic_target"] %}
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
    count(distinct case when r.transaction_type = 'invoice' and r.transaction_code not like '%HDD%' then r.transaction_id end) as num_invoices,
    sum(case when r.transaction_type = 'invoice' and r.transaction_code not like '%HDD%' then r.total end) as invoice_value,
  FROM
    {{ ref("fct__transactions") }} r
  WHERE 1=1
    {% if is_incremental() %}
      and DATE(r.transaction_date) >= date_add(CURRENT_DATE,INTERVAL -7 DAY) 
      {# r.transaction_date >='2024-02-01' #}
    {% else %}
      and r.transaction_date >= '2023-01-01'
    {% endif %}
    AND r.branch_id NOT IN (1000087891) 
    {# and asm.asm_name is not null #}
    {# and asm.channel = 'Offline' #}
GROUP BY 1
),

budget AS (
  SELECT
    {# budget.branch_id,budget.date,#}
    {{ dbt_utils.generate_surrogate_key(['branch_id', 'date']) }} AS branch_working_day_id,
    {% for target in targets %}
      SUM({{'daily_'+target }}) AS  {{ 'daily_'+target }},
    {% endfor %}
  FROM
    {{ ref("facebook_budget") }} budget
  WHERE
    budget.date <= CURRENT_DATE()
  {% if is_incremental() %}
  AND budget.date >= date_add(CURRENT_DATE,INTERVAL -7 DAY) 
  {% endif %}
  GROUP BY
    1
),
operating_days AS (
  SELECT
    *
  FROM
    {{ ref("int__working_days") }}
  WHERE
    1 = 1
  {% if is_incremental() %}
  AND DATE(DATE) >= date_add(CURRENT_DATE,INTERVAL -7 DAY)
  {% else %}
    AND DATE >= '2023-01-01'
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
)

SELECT
  o.* EXCEPT(branch_working_day_id),
  b.* EXCEPT(branch_working_day_id),
  t.* except(branch_name, date),
  ops.date,
  ops.branch_id,
  ops.promotion,
FROM operating_days ops
left outer join offline_performance o on ops.branch_working_day_id = o.branch_working_day_id
left outer join budget b on ops.branch_working_day_id = b.branch_working_day_id
left outer join _traffic t on ops.branch_name = t.branch_name and ops.date = t.date


{# offline_performance o full
  OUTER JOIN budget b
  ON o.branch_id = b.branch_id
  AND o.transaction_date = b.date full
  OUTER JOIN operating_days C
  ON COALESCE(
    o.branch_id,
    b.branch_id
  ) = C.branch_id
  AND COALESCE(
    o.transaction_date,
    b.date
  ) = C.date #}