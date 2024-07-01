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
    {{ ref("fct__transactions") }} r
    INNER JOIN {{ ref("dim__branches") }} asm
     ON r.branch_id = asm.branch_id
  WHERE
    {% if is_incremental() %}
      date(r.transaction_date) >= date_add(date(_dbt_max_partition), interval -3 day)
      {# r.transaction_date >='2024-02-01' #}
    {% else %}
      r.transaction_date >= '2023-01-01'
    {% endif %}
    AND r.branch_id NOT IN (1000087891)
    and asm.asm_name is not null
    and asm.channel = 'Offline'
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
     {% if is_incremental() %}
      and budget.date >= date_add(date(_dbt_max_partition), interval -3 day)
      {# and budget.date >= '2024-02-01' #}
    {% endif %}
  GROUP BY
    1,
    2
)

SELECT
  o.* except(branch_id,transaction_date),
  b.* except(branch_id, date),
  coalesce(o.transaction_date,b.date) as date,
coalesce(o.branch_id,b.branch_id) as branch_id,
FROM
  offline_performance o
  full outer join budget b on o.branch_id = b.branch_id and o.transaction_date = b.date