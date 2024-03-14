{{
  config(
    tags=['view', 'dimension','nhanhvn']
  )
}}

WITH source as (
    select * 
    from {{source(
        'nhanhvn',
        'p_customers_*'
    )}}

),

deduplicate as (
{{ dbt_utils.deduplicate(
    relation = ,
    partition_by = 'id',
    order_by = "_batched_at desc",
) }}
)

SELECT
    safe_cast(id as int64) AS customer_id,
    NAME AS customer_name,
    email AS email,
    mobile AS contact_number,
    case when gender = '1' then 'm' when gender  = '2' then 'f' end as gender,
    address,
    birthday,
    safe_cast(totalMoney as int64) AS total_money,
    DATE(startedDate) AS first_purchase_date,
    safe_cast(startedDepotId as int64) AS first_purchase_depot_id,
    safe_cast(points as int64) as points,
    safe_cast(totalBills as int64) AS total_bills,
    DATE(lastBoughtDate) AS last_bought_date,
FROM
    source
