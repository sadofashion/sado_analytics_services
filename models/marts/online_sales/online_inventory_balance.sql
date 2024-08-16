{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'date',
  'data_type': 'date',
  'granularity': 'day' },
  incremental_strategy = 'insert_overwrite',
  unique_key = 'concat(product_id,date)',
  on_schema_change = 'sync_all_columns',
  tags = ['incremental', 'daily','nhanhvn'],
  enabled=false
) }}

WITH inventory AS (

  SELECT
    *
  EXCEPT(
      daily_beginning_remain,
      daily_beginning_available
    ),
    COALESCE(LAG(daily_ending_remain) over (PARTITION BY product_id
  ORDER BY
    DATE ASC), daily_beginning_remain) daily_beginning_remain,
    COALESCE(LAG(daily_ending_available) over (PARTITION BY product_id
  ORDER BY
    DATE ASC), daily_beginning_available) daily_beginning_available,
  FROM
    (
      SELECT
        DISTINCT product_id,
        DATE(updated_at) AS DATE,
        FIRST_VALUE(DATE(updated_at) ignore nulls) over w2 AS first_inventory_date,
        FIRST_VALUE(
          remain ignore nulls
        ) over w1 AS daily_ending_remain,
        FIRST_VALUE(
          remain ignore nulls
        ) over w2 AS daily_beginning_remain,
        FIRST_VALUE(
          available ignore nulls
        ) over w1 AS daily_ending_available,
        FIRST_VALUE(
          available ignore nulls
        ) over w2 AS daily_beginning_available,
      FROM
        {{ ref('stg_nhanhvn__inventories') }}
      WHERE
        updated_at >= '2023-09-20'
        AND depot_name = 'KHO ONLINE HÀ NỘI' window w1 AS (
          PARTITION BY product_id,
          DATE(updated_at)
          ORDER BY
            updated_at DESC
        ),
        w2 AS (
          PARTITION BY product_id,
          DATE(updated_at)
          ORDER BY
            updated_at ASC
        )
    )
  WHERE
    daily_ending_remain IS NOT NULL
),
calendar AS (
  SELECT
    p.product_id,
    C.date,
  FROM
    {{ ref('calendar') }} C
    CROSS JOIN {{ ref('stg_nhanhvn__products') }}
    p
    CROSS JOIN {{ ref('stg_nhanhvn__depots') }}
    d
  WHERE
    C.date <= CURRENT_DATE()
    AND C.date >= '2023-09-20'
),
transform AS (
  SELECT
    COALESCE(
      inv.product_id,
      C.product_id
    ) AS product_id,
    inv.date AS inv_date,
    C.date AS c_date,
    CASE
      WHEN inv.date = C.date THEN inv.daily_ending_remain
    END AS daily_ending_remain,
    CASE
      WHEN inv.date = C.date THEN inv.daily_beginning_remain
    END AS daily_beginning_remain,
    CASE
      WHEN inv.date = C.date THEN inv.daily_ending_available
    END AS daily_ending_available,
    CASE
      WHEN inv.date = C.date THEN inv.daily_beginning_available
    END AS daily_beginning_available,
  FROM
    calendar C
    LEFT JOIN inventory inv
    ON C.product_id = inv.product_id
    AND C.date = inv.date
    AND C.date >= inv.first_inventory_date
),
daily_inventory AS (
  SELECT
    DISTINCT product_id,
    c_date AS DATE,
    COALESCE(
      daily_ending_remain,
      FIRST_VALUE(
        daily_ending_remain ignore nulls
      ) over w1
    ) AS daily_ending_remain,
    COALESCE(
      daily_beginning_remain,
      FIRST_VALUE(
        daily_ending_remain ignore nulls
      ) over w1
    ) AS daily_beginning_remain,
    COALESCE(
      daily_ending_available,
      FIRST_VALUE(
        daily_ending_available ignore nulls
      ) over w1
    ) AS daily_ending_available,
    COALESCE(
      daily_beginning_available,
      FIRST_VALUE(
        daily_ending_available ignore nulls
      ) over w1
    ) AS daily_beginning_available,
  FROM
    transform
  WHERE
    1 = 1 window w1 AS (
      PARTITION BY product_id
      ORDER BY
        c_date DESC rows BETWEEN CURRENT ROW
        AND unbounded following
    )
),
daily_sales AS (
  SELECT
    product_id,
    DATE(created_date) AS DATE,
    SUM(quantity) AS daily_qty_sold
  FROM
    {{ ref('orders_items') }}
  GROUP BY
    1,
    2
),
daily_bills AS (
  SELECT
    product_id,
    DATE(bill_date) bill_date,
    SUM(
      CASE
        WHEN bill_type = 'Nhập kho' THEN quantity
      END
    ) quantity_in,
    SUM(
      CASE
        WHEN bill_type = 'Xuất kho' THEN quantity
      END
    ) quantity_out,
    SUM(
      CASE
        WHEN bill_type = 'Xuất kho'
        AND bill_mode = 'Giao hàng' THEN quantity
      END
    ) quantity_out_issue,
    SUM(
      CASE
        WHEN bill_type = 'Xuất kho'
        AND bill_mode <> 'Giao hàng' THEN quantity
      END
    ) quantity_out_adjust,
  FROM
    {{ ref('int_nhanhvn__bills_reference_orders') }}
  WHERE
    bill_date >= '2023-09-20'
  GROUP BY
    1,
    2
)
SELECT
  inv.*,
  COALESCE(
    s.daily_qty_sold,
    0
  ) daily_qty_sold,
  (
    inv.daily_ending_remain + inv.daily_beginning_remain
  ) / 2 AS daily_average_remain,
  (
    inv.daily_ending_available + inv.daily_beginning_available
  ) / 2 AS daily_average_available,
  COALESCE(
    b.quantity_in,
    0
  ) quantity_in,
  COALESCE(
    b.quantity_out,
    0
  ) quantity_out,
  COALESCE(
    b.quantity_out_issue,
    0
  ) quantity_out_issue,
  COALESCE(
    b.quantity_out_adjust,
    0
  ) quantity_out_adjust,
FROM
  daily_inventory inv
  LEFT JOIN daily_sales s
  ON inv.product_id = s.product_id
  AND inv.date = s.date
  LEFT JOIN daily_bills b
  ON inv.product_id = b.product_id
  AND inv.date = b.bill_date