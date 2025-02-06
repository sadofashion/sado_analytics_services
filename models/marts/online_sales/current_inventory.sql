{{ config(
    tags = ['table', 'fact','nhanhvn'],
    enabled=false
) }}

{{ dbt_utils.deduplicate(
    relation = ref('stg_nhanhvn__inventories'), 
    partition_by = 'product_id,depot_name', 
    order_by = "updated_at desc",
    ) 
    }}
