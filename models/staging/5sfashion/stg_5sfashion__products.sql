{{
    config(
        tags=['website','dimensions','view']
    )
}}

with products as (
    {{dbt_utils.deduplicate(
        relation = source(
            '5sfashion',
            'products'
        ),
        partition_by = '_id',
        order_by = '_batched_at desc'
    )}}
)

SELECT
_id as product_id,
name as product_name,
case product_type_id
    when "63e5ee4fa056b1c6920ed269" then 'Áo'
    when "63e5ee4fa056b1c6920ed269" then "Quần"
    when "63e5ee4fa056b1c6920ed26b" then "Khác"
    else "Chưa phân loại" end as product_type,
keyword as product_keyword,
updated_at,
created_at,
published_at,
regexp_replace(code,r'\W','') as product_code,
categories,
FROM
    products
{# left join unnest(categories) categories #}
where _id is not null