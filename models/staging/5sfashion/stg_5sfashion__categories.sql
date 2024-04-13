{{ config(
    tags = ['website','dimensions','view']
) }}

WITH categories AS (
    {{ dbt_utils.deduplicate(
        relation = source(
            '5sfashion',
            'categories'
        ),
        partition_by = '_id',
        order_by = '_batched_at desc'
    ) }}
),
menu_items AS (
    {{ dbt_utils.deduplicate(
        relation = source(
            '5sfashion',
            'cms_menu_items'
        ),
        partition_by = '_id',
        order_by = '_batched_at desc'
    ) }}
),

menu_categoried as (
    SELECT
        c1.target_value,
        c1.title as parent_category,
        c1.depth,
        coalesce(c2.title,c1.title) AS sub_productline,
        c2.depth AS sub_productline_depth,
        coalesce(c3.title,c2.title, c1.title) AS product_line,
        c3.depth AS product_line_depth,
    from menu_items c1
    LEFT JOIN menu_items c2
        ON c1.parent_id = c2._id
    LEFT JOIN menu_items c3
        ON c2.parent_id = c3._id
    WHERE
        c1.cms_menu_id = "63e5ee4fa056b1c6920ed1df"
        {# and (c2.parent_id is not null or c2.title = 'PHỤ KIỆN') #}
)

select 
m.* except(target_value),
c._id as category_id,
c.name as category,
from menu_categoried m
left join categories c on m.target_value = c._id