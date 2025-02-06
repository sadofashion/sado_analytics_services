{{
    config(
        tags=['website','dimensions','view'],
        enabled = false
    )
}}

with base_products as (
    {{dbt_utils.deduplicate(
        relation = source(
            'web',
            'products'
        ),
        partition_by = 'id',
        order_by = '_batched_at desc'
    )}}
),
base_product_details as (
    {{dbt_utils.deduplicate(
        relation = source(
            'web',
            'product_details'
        ),
        partition_by = 'id',
        order_by = '_batched_at desc'
    )}}
),
base_categories as (
    {{dbt_utils.deduplicate(
        relation = source(
            'web',
            'categories'
        ),
        partition_by = 'id',
        order_by = '_batched_at desc'
    )}}
)

select * 
from base_product_details pd 
left join base_products p on pd.product_id = p.id
