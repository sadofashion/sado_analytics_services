{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'date',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'insert_overwrite',
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'fact','dashboard']
) }}

{% set metrics = ["impressions","spend","clicks","reach","link_click","post_engagement","offline_conversion_purchase","offline_conversion_purchase_value","pixel_purchase","pixel_purchase_value","meta_purchase","meta_purchase_value"] %}
{% set targets = ["budget", "sales_target", "traffic_target"] %}
{% set rev_calcols ={ "transaction_id" :"count(distinct ",
"total" :"sum(",
"total_payment" :"sum(" } %}
{% set rev_types = ["invoice", "return"] %}

with offline_performance as (
  SELECT
  asm.local_page_code, 
  asm.fb_ads_pic AS pic,
  DATE(r.transaction_date) transaction_date,
    {%- for col,cal in rev_calcols.items() %}
      {{ cal }} {{ col }}{{ ")" }} AS val_{{ col }},
      {%- for type in rev_types %}
        {{ cal }}
        CASE
          WHEN transaction_type = '{{type}}' THEN {{ col }}
        END {{ ")" }} AS num_{{ type }}_{{ col }},
      {% endfor -%}
    {% endfor -%}
    COUNT(
      DISTINCT r.branch_id
    ) AS num_stores,
  FROM
    {{ ref("fct__transactions") }} r
    INNER JOIN {{ ref("dim__branches") }} asm
    ON r.branch_id = asm.branch_id
  WHERE
  {%- if is_incremental() %}
    date(r.transaction_date) >= date_add(current_date, interval -7 day)
  {% else %}
    r.transaction_date >= '2023-11-01'
  {% endif -%}
    AND r.branch_id NOT IN (1000087891)
    and asm.asm_name is not null
    and asm.channel = 'Offline'
  {{dbt_utils.group_by(3)}} 
),

facebook_budget AS (
  SELECT
    budget.local_page_code,
    budget.date,
    {%- for target in targets %}
      SUM(daily_{{ target }}) AS daily_{{ target }},
    {% endfor -%}
  FROM
    {{ ref("facebook_budget") }}
    budget
  WHERE
    budget.date <= CURRENT_DATE()
    {%- if is_incremental() %}
    and budget.date >= date_add(current_date, interval -7 day)
    {% else %}
    and budget.date >= '2023-11-01'
  {% endif -%}
  {{dbt_utils.group_by(2)}}
),
facebook_performance as (
  SELECT
    cp.ad_group_location,
    cp.ad_pic,
    fb.date_start,
    {% for metric in metrics %}
      SUM(fb.{{ metric }}) AS {{ metric }},
    {% endfor %}
  FROM
    {{ ref("stg_facebookads__adsinsights") }} fb
    left join {{ref("dim__campaigns")}} cp on fb.ad_key = cp.ad_key
  WHERE 
  {% if is_incremental() %}
    date_start >= date_add(current_date, interval -7 day)
  {% else %}
    date_start >= '2023-11-01'
  {% endif %}
  {# and cp.ad_group_location = 'Store' #}
  and fb.account_id not in (
    -- wookids
    311864311227191,622771789982135,3744530109108893,
    -- ecom
    521606912204785,151869866811869,
    -- HR
    572603800939181,
    -- branding
    836481701438037
    )
  {{dbt_utils.group_by(3)}}
)

select f.* except(date_start,ad_group_location,ad_pic),
  o.* except(transaction_date,local_page_code,pic),
  b.* except(date,local_page_code),
  coalesce(f.date_start, o.transaction_date,b.date) as date,
  coalesce(f.ad_group_location, o.local_page_code, b.local_page_code) as local_page_code,
  coalesce(f.ad_pic,o.pic) as ad_pic
from facebook_performance f
full outer join  offline_performance o 
on f.date_start = o.transaction_date 
and f.ad_group_location = o.local_page_code 
left join facebook_budget b
on coalesce(f.date_start,o.transaction_date) = b.date
and coalesce(f.ad_group_location,o.local_page_code) = b.local_page_code
