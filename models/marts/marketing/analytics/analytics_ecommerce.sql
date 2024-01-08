{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'event_date',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'merge',
  unique_key = 'concat(event_id,item_id)',
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'daily','GA4']
) }}

WITH raw_ AS (

  SELECT
    event_id,
    event_date,
    event_timestamp,
    client_id,
    event_name,
    param_value,
    param_key,
    items.item_id,
    items.quantity,
    items.price,
    items.item_variant,
    items.item_name,
  FROM
    {{ ref('stg_analytics__events') }}
    LEFT JOIN unnest(items) items
  WHERE
    1 = 1
    AND event_name IN (
      'purchase',
      'add_to_cart',
      'add_shipping_info',
      'remove_from_cart',
      'begin_checkout',
      'view_cart',
      'select_item',
      'view_item'
    )

{% if is_incremental() %}
AND (event_date >= DATE(_dbt_max_partition)
OR event_date >= date_sub(CURRENT_DATE(), INTERVAL 2 DAY))
{% endif %}),
data_ AS (
  SELECT
    event_id,
    event_date,
    event_timestamp,
    client_id,
    event_name,
    item_id,
    quantity,
    price,
    item_variant,
    item_name,
    val_payment_type AS payment_type,
    val_transaction_id AS transaction_id,
    val_shipping AS shipping,
    val_delivery_method AS delivery_method,
    val_value AS value,
    COALESCE(
      regexp_extract(
        val_page_location,
        r'(?:[a-zA-Z]+://)?(?:[a-zA-Z0-9-.]+){1}(/[^\?#;&]+)'
      ),
      '/'
    ) AS page_path,
    CONCAT(
      val_ga_session_id,
      '-',
      client_id
    ) AS session_id,
    ROW_NUMBER() over (
      PARTITION BY (
        CASE
          WHEN event_name = 'purchase' THEN val_transaction_id
          ELSE event_id
        END
      )
      ORDER BY
        event_timestamp ASC
    ) AS rn_
  FROM
    raw_ AS r pivot (ANY_VALUE(param_value) AS val FOR param_key IN ('ga_session_id', 'page_location', 'payment_type', 'transaction_id', 'shipping', 'delivery_method', 'value'))
)
SELECT
  *
EXCEPT(rn_)
FROM
  data_
WHERE
  1 = 1 -- and  rn_ = 1
