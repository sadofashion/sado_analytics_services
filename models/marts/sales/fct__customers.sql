{# {{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    )
}} #}

with kiotviet_cus as (
    select 
    branch_id as establised_branch_id,
    customer_id as kiotviet_customer_id,
    contact_number, 
    customer_name,
    gender, 
    date(birth_date) birth_date, 
    birth_month,
    date(created_date) created_date,
    date(modified_date) as last_modified_date,
    customer_groups, 
    customer_recency_group
    from {{ ref('stg_kiotviet__customers') }}
    where regexp_contains(contact_number, r'^0\d{9}$')
    {% if is_incremental() %}
     and modified_date >= date(_dbt_max_partition)
    {% endif %}
),
nhanhvn_cus as (
    select
    customer_id as nhahnvn_customer_id,
    customer_name,
    email,
    contact_number,
    gender,
    address,
    birthday,
    first_purchase_date,
    last_bought_date as last_modified_date
    from {{ ref("stg_nhanhvn__customers") }}
    where regexp_contains(contact_number, r'^0\d{9}$')
    {% if is_incremental() %}
      and date(last_bought_date) >= date(_dbt_max_partition)
    {% endif %}
)
{# , #}

{# fixed_attributes as ( #}
    select 
coalesce(k.contact_number, n.contact_number) contact_number,
k.kiotviet_customer_id,
n.nhahnvn_customer_id,
coalesce(k.customer_name,n.customer_name) customer_name,
coalesce(k.gender,n.gender) gender,
coalesce(k.birth_date,date(n.birthday)) birth_date,
coalesce(k.birth_month,extract(month from date(n.birthday))) birth_month,
date_diff(current_date(),coalesce(k.birth_date,date(n.birthday)),day) as age,
coalesce(k.created_date,n.first_purchase_date) created_date,
coalesce(k.last_modified_date,n.last_modified_date) as last_modified_date,
n.address,
from kiotviet_cus k 
full outer join nhanhvn_cus n on k.contact_number = n.contact_number
{# where 1=1
{% if is_incremental() %}
  and coalesce(k.last_modified_date,n.last_modified_date) >= date(_dbt_max_partition)
{% endif %} #}
{# ),
dynamic_attributes as (
  SELECT distinct
  customer_id,
  t.source,
  COUNT(transaction_source_id) over w1 AS num_transactions,
  SUM(total_payment) over w1 AS total_revenue,
  MAX(transaction_date) over w1 AS last_purchase,
  SUM(CASE
      WHEN transaction_type = 'invoice' THEN total_payment
  END
    ) over w1 AS total_invoice,
  first_value(branch_id ignore nulls) over w2 as first_purchase_branch,
  last_value(branch_id ignore nulls) over w2 as last_purchase_branch,
 FROM
  {{ ref('fct__transactions') }} t
  window w1 as (partition by customer_id,t.source),
  w2 as (partition by customer_id order by transaction_date asc rows between unbounded preceding and unbounded following)
)

select f.*,
first_purchase_branch,
last_purchase_branch,
max(d.source = 'nhanhvn') as has_online_purchase,
max(d.source = 'kiotviet') as has_offline_purchase,
sum(d.num_transactions) num_transactions,
sum(d.total_revenue) total_revenue,
max(d.last_purchase) last_purchase,
sum(d.total_invoice) total_invoice,
from dynamic_attributes d 
cross join fixed_attributes f
where
  (f.kiotviet_customer_id = d.customer_id and d.source = 'kiotviet') 
or (f.nhahnvn_customer_id = d.customer_id and d.source = 'nhanhvn')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13 #}