{{
  config(
    tags = ["fact","kiotviet","inventory"]
    )
}}

with cal_ending_inventory as (
SELECT
    distinct 
    product_id,
    branch_id,
    DATE(_batched_at) AS inventory_date,
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
    coalesce(max(case when inventory_date > last_inventory_date and ending_inventory = 0 then inventory_date end ) over (PARTITION BY product_id,branch_id),last_inventory_date) last_inventory_date ,
    lead(inventory_date) over (PARTITION BY product_id,branch_id ORDER BY inventory_date) as next_inventory_date,
from cal_ending_inventory
)

select inv.* , w.date
from cal_last_inventory_date inv
left join {{ ref("int__working_days") }} w
on inv.branch_id = w.branch_id 
and ((inv.inventory_date <= w.date and inv.next_inventory_date > w.date) 
     or (inv.last_inventory_date <= w.date and w.date< current_date() and ending_inventory >0 and inv.next_inventory_date is null)
     or (inv.last_inventory_date = w.date and inv.next_inventory_date is null and ending_inventory =0)
    )
    
where inventory_date <= last_inventory_date