{{ config(
  materialized = 'incremental',
  unique_key = 'customer_id',
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'daily','nhanhvn','dimension'],
  incremental_strategy = 'merge',
  partition_by ={ "field": "last_bought_date",
  "data_type": "date",
  "granularity": "day" }
) }}

WITH source AS (

  SELECT
    *
  FROM
    {{ source(
      'nhanhvn',
      'p_customers'
    ) }}
  WHERE
    1 = 1
{% if is_incremental() %}
AND parse_date('%Y%m%d',_TABLE_SUFFIX) >= date_add(DATE(_dbt_max_partition), INTERVAL -2 DAY)
{% endif %}
),

deduplicate AS (
  {{ dbt_utils.deduplicate(
    relation = 'source',
    partition_by = 'id',
    order_by = "_batched_at desc",
  ) }}
)

SELECT
  safe_cast(id AS int64) AS customer_id,
  NAME AS customer_name,
  email AS email,
  mobile AS contact_number,
  CASE
    WHEN gender = '1' THEN 'm'
    WHEN gender = '2' THEN 'f'
  END AS gender,
  address,
  birthday,
  safe_cast(totalMoney AS int64) AS total_money,
  DATE(startedDate) AS first_purchase_date,
  safe_cast(startedDepotId AS int64) AS first_purchase_depot_id,
  safe_cast(points AS int64) AS points,
  safe_cast(totalBills AS int64) AS total_bills,
  DATE(lastBoughtDate) AS last_bought_date,
FROM
  deduplicate
