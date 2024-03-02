{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'date',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'merge',
  unique_key = ['date','page'],
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'fact','dashboard']
) }}

{% set metrics = ["impressions","spend","clicks","reach","link_click","post_engagement","offline_conversion_purchase","offline_conversion_purchase_value","pixel_purchase","pixel_purchase_value","meta_purchase","meta_purchase_value","_results_message"] %}
{% set targets = ["budget", "sales_target", "traffic_target"] %}
{% set rev_calcols ={ "transaction_id" :"count(distinct ",
"total" :"sum(",
"total_payment" :"sum(" } %}
{% set rev_types = ["invoice", "return"] %}


WITH facebook_performance AS (

  SELECT
    fb.page,
    fb.page_type,
    fb.date_start,
    fb.pic,
    {% for metric in metrics %}
      SUM(fb.{{ metric }}) AS {{ metric }},
    {% endfor %}
  FROM
    {{ ref("facebook_performance") }}
    fb
  WHERE 
  {% if is_incremental() %}
    date_start >= date_add(date(_dbt_max_partition), interval -3 day)
  {% else %}
    date_start >= '2023-11-01'
  {% endif %}
    
  and fb.page_type in ('local_page','region_page','compiled')
  and account_name not in ('CĐ WEB')
  GROUP BY
    1,
    2,3,4
),
facebook_budget AS (
  SELECT
    budget.local_page,
    budget.region_page,
    budget.date,
    budget.milestone_name,
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
  {% endif %}
  GROUP BY
    1,
    2,
    3,4
),
offline_performance AS (
  SELECT
  asm.local_page, 
  asm.region_page,
    asm.fb_ads_pic AS pic,
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

    COUNT(
      DISTINCT r.branch_id
    ) AS num_stores,
  FROM
    {{ ref("revenue") }}
    r
    INNER JOIN {{ ref("dim__offline_stores") }} asm
    ON r.branch_id = asm.branch_id
  WHERE
       {% if is_incremental() %}
    date(r.transaction_date) >= date_add(date(_dbt_max_partition), interval -3 day)
  {% else %}
    r.transaction_date >= '2023-11-01'
  {% endif %}
    AND r.branch_id NOT IN (1000087891)
    and asm.asm_name is not null
  GROUP BY
    1,
    2,
    3,4
),
asms AS (
  SELECT
    DISTINCT asm.asm_name,
    asm.local_page,
    asm.region_page,
    asm.fb_ads_pic,
  FROM
    {{ ref("dim__offline_stores") }} asm
)
SELECT
  DISTINCT 
  p.* EXCEPT(page,date_start,pic),
  o.* EXCEPT(local_page,region_page,transaction_date,pic),
  b.* EXCEPT(date,local_page,region_page,milestone_name),
  COALESCE(
    p.date_start,
    o.transaction_date,
    b.date
  ) AS date,
  COALESCE(
    p.page,
    b.local_page,
    a1.local_page,
    a2.region_page
  ) AS page,
  coalesce(a1.asm_name,a2.asm_name) asm_name,
  COALESCE(
    a1.fb_ads_pic,
    a2.fb_ads_pic,
p.pic
  ) AS pic,
FROM
  facebook_performance p 
  full outer JOIN facebook_budget b
  ON p.date_start = b.date
  AND (
    (lower(p.page) = lower(b.local_page) and p.page_type='local_page')
    or (lower(p.page) = lower(b.region_page) and p.page_type = 'region_page')
  )
  full outer JOIN offline_performance o
  ON o.transaction_date = coalesce(p.date_start,b.date)
  AND (
    (lower(o.local_page) = lower(p.page) AND p.page_type = 'local_page' )
    or (lower(o.region_page) = lower(p.page) and p.page_type='region_page')
    or (lower(o.local_page) = lower(b.local_page) and p.page_type is null)
    {# or (lower(o.region_page) = lower(b.region_page) and lower(o.region_page) <> lower(o.local_page) and p.page_type is null) #}
  )
  LEFT JOIN asms as a1
  ON lower(COALESCE(
    p.page,
    b.local_page,
    o.local_page
  )) = lower(a1.local_page) 
  left join asms as a2 on ( LOWER(COALESCE( p.page, b.region_page,o.region_page )) = LOWER(a2.region_page) )
  