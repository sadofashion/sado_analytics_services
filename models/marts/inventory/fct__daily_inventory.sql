{{
  config(
    tags = ["fact","kiotviet","inventory"]
    )
}}

with cal_ending_inventory as (
SELECT
    distinct 
    product_id,
    product_code,
    branch_id,
    DATE(_batched_at) AS inventory_date,
    max(on_hand) over w1 AS max_daily_inventory,
    LAST_VALUE(on_hand) over w1 AS ending_inventory,
    max(case when on_hand > 0 then DATE(_batched_at) end) over w2 as last_inventory_date
FROM
    {{ ref("stg_kiotviet__inventories") }}
    window w1 AS (
        PARTITION BY product_id,
        branch_id,
        DATE(_batched_at)
        ORDER BY
            _batched_at rows BETWEEN unbounded preceding
            AND unbounded following
    ),
    w2 as (
        PARTITION BY product_id,
        branch_id
        ORDER BY
            _batched_at rows BETWEEN unbounded preceding
            AND unbounded following
    )
),
cal_last_inventory_date as (
select * except(last_inventory_date),
min(case when inventory_date > last_inventory_date and ending_inventory = 0 then inventory_date end ) over (PARTITION BY product_id,branch_id) last_inventory_date

from cal_ending_inventory
)

select * from cal_last_inventory_date 
where inventory_date <= last_inventory_date