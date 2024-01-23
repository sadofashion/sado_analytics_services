{{ config(
  materialized = 'incremental',
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
"total" :"sum(",
"total_payment" :"sum(" } %}
{% set rev_types = ["invoice", "return"] %}

with offline_performance AS (
  SELECT
    r.branch_id,
    DATE(
      r.transaction_date
    ) transaction_date,
    {% for col,cal in rev_calcols.items() %}
      {{ cal }} {{ col }}{{ ")" }} AS val_{{ col }},
      {% for type in rev_types %}
        {{ cal }}
        CASE
          WHEN transaction_type = '{{type}}' THEN {{ col }}
        END {{ ")" }} AS num_{{ type }}_{{ col }},
      {% endfor %}
    {% endfor %}
    
    COUNT( DISTINCT r.branch_id) AS num_stores,
    FROM
    {{ ref("revenue") }}
    r
  WHERE
    r.transaction_date >= '2023-01-01'
  GROUP BY
    1,
    2
),

budget AS (
  SELECT
    budget.branch_id,
    budget.date,
    {% for target in targets %}
      SUM(
        daily_{{ target }}
      ) AS daily_{{ target }},
    {% endfor %}
  FROM
    {{ ref("facebook_budget") }}
    budget
  WHERE
    budget.date <= CURRENT_DATE()
  GROUP BY
    1,
    2
)

SELECT
  o.* except(branch_id,transaction_date),
  b.* except(branch_id, date),
  o.transaction_date as date,
o.branch_id,
FROM
  offline_performance o
  left outer join budget b on o.branch_id = b.branch_id and o.transaction_date = b.date