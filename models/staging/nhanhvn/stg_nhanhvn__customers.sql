{{
  config(
    materialized = 'incremental',
    unique_key = 'customer_id',
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'daily','nhanhvn','dimension'],
    incremental_strategy = 'merge',
    partition_by ={ "field": "date",
    "data_type": "timestamp",
    "granularity": "day" }
    )
}}

WITH source as (
    select * 
    from {{source(
        'nhanhvn',
        'p_customers'
    )}}
    where 1=1
    {% if is_incremental() %}
      and parse_date('%Y%m%d', _TABLE_SUFFIX) >= date_add(date(_dbt_max_partition), interval -2 day)
    {% endif %}
),

deduplicate as (
{{ dbt_utils.deduplicate(
    relation = 'source',
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
    deduplicate
