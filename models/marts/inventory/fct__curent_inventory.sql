{{
  config(
    tags = ["fact","kiotviet","inventory"]
    )
}}

with source as ({{ 
    dbt_utils.deduplicate(
    relation = ref('stg_kiotviet__inventories'), 
    partition_by = 'product_id,branch_id', 
    order_by = "_batched_at desc",
    ) 
    }})
select * from source where on_hand > 0