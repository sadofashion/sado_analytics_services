{{
  config(
    materialized = 'table',
    tags = ['table', 'daily','dimension','kiotviet','nhanhvn']
    )
}}

with kiotviet_cus as (
    select 
    customer_id as kiotviet_customer_id,
    contact_number, 
    customer_name,
    gender, 
    date(birth_date) birth_date, 
    birth_month,
    date(created_date) created_date,
    date(modified_date) as last_modified_date,
    from {{ ref('stg_kiotviet__customers') }}
    where regexp_contains(contact_number, r'^0\d{9}$')
    {% if is_incremental() %}
     and modified_date >= date(_dbt_max_partition)
    {% endif %}
    qualify ROW_NUMBER() over( PARTITION BY contact_number ORDER BY modified_date DESC) = 1
),
nhanhvn_cus as (
    select
    customer_id as nhanhvn_customer_id,
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
    qualify ROW_NUMBER() over( PARTITION BY contact_number ORDER BY last_bought_date DESC) = 1
)

    select 
coalesce(k.contact_number, n.contact_number) contact_number,
coalesce(k.kiotviet_customer_id,n.nhanhvn_customer_id) as universal_customer_id,
k.kiotviet_customer_id,
n.nhanhvn_customer_id,
coalesce(k.customer_name,n.customer_name) customer_name,
coalesce(k.gender,n.gender) gender,
coalesce(k.birth_date,date(n.birthday)) birth_date,
coalesce(k.birth_month,extract(month from date(n.birthday))) birth_month,
date_diff(current_date(),coalesce(k.birth_date,date(n.birthday)),day) as age,
coalesce(k.created_date,n.first_purchase_date) created_date,
greatest(k.last_modified_date,n.last_modified_date) as last_modified_date,
n.address,
from kiotviet_cus k 
full outer join nhanhvn_cus n on k.contact_number = n.contact_number
