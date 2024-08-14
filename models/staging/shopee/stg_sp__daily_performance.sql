with source as (
    {{
        dbt_utils.deduplicate(
            relation = source('shopee', 'order_list'), 
            partition_by = 'order_sn', 
            order_by = '_batched_at desc'
            )
            }}
)