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
{% set rev_calcols ={ "transaction_id" :"count(distinct",
"total" :"sum(",
"total_payment" :"sum(" } %}
{% set rev_types = ["invoice", "return"] %}
WITH facebook_performance AS (

  SELECT
    fb.page,
    date_start,
    {# fb.pic, #}
    {% for metric in metrics %}
      SUM(
        fb.{{ metric }}
      ) AS {{ metric }},
    {% endfor %}
  FROM
    {{ ref("facebook_performance") }}
    fb
  WHERE
    date_start >= '2023-11-01'
    AND (
      fb.page IN (
        SELECT
          DISTINCT A.new_ads_page
        FROM
          {{ ref("dim__offline_stores") }} A
      )
      OR fb.page IN (
        SELECT
          DISTINCT A.old_ads_page
        FROM
          {{ ref("dim__offline_stores") }} A
      )
      OR fb.page IN (
        "5SFTHA",
        "5SFTIE",
        "5SFTUN",
        "5SFTRA",
        "5SFT",
        "5SFG",
        "5SF"
      )
    )
  GROUP BY
    1,
    2
),
facebook_budget AS (
  SELECT
    budget.page,
    budget.date,
    budget.milestone_name,
    {# budget.pic, #}
    {% for target in targets %}
      SUM(
        daily_ {{ target }}
      ) AS daily_ {{ target }},
    {% endfor %}
  FROM
    {{ ref("facebook_budget") }}
    budget
  WHERE
    budget.date <= CURRENT_DATE()
  GROUP BY
    1,
    2,
    3
),
offline_performance AS (
  SELECT
    A.new_ads_page AS page,
    A.new_ads_pic AS pic,
    DATE(
      r.transaction_date
    ) transaction_date,
    {% for col,
      cal in rev_calcols.items() %}
      {{ cal }}
      {{ col }}
      {{ ")" }} AS val_ {{ col }},
      {% for type in rev_types %}
        {{ cal }}
        CASE
          WHEN transaction_type = '{{type}}' THEN {{ col }}
        END {{ ")" }} AS num_ {{ type }}
        _ {{ col }},
      {% endfor %}
    {% endfor %}

    COUNT(
      DISTINCT r.branch_id
    ) AS num_stores,
  FROM
    {{ ref("revenue") }}
    r
    INNER JOIN {{ ref("dim__offline_stores") }} A
    ON r.branch_id = A.branch_id
  WHERE
    r.transaction_date >= '2023-11-01'
    AND r.branch_id NOT IN (1000087891)
  GROUP BY
    1,
    2,
    3
),
asms AS (
  SELECT
    DISTINCT A.asm_name,
    A.new_ads_page AS page,
    A.new_ads_pic AS pic,
    A.old_ads_page AS old_page,
    A.old_ads_pic AS old_pic,
  FROM
    {{ ref("dim__offline_stores") }} A
)
SELECT
  DISTINCT p.*
EXCEPT(
    page,
    date_start
  ),
  o.*
EXCEPT(
    page,
    transaction_date,
    pic
  ),
  b.*
EXCEPT(
    DATE,
    page,
    milestone_name
  ),
  COALESCE(
    p.date_start,
    o.transaction_date,
    b.date
  ) AS DATE,
  COALESCE(
    p.page,
    o.page,
    b.page
  ) AS page,
  asms.asm_name,
  COALESCE(
    asms.pic,
    o.pic
  ) AS pic,
FROM
  facebook_performance p full
  JOIN facebook_budget b
  ON p.date_start = b.date
  AND (
    p.page = b.page
  ) full
  JOIN offline_performance o
  ON o.transaction_date = p.date_start
  AND (
    o.page = p.page
  )
  LEFT JOIN asms
  ON COALESCE(
    p.page,
    o.page,
    b.page
  ) = asms.page
