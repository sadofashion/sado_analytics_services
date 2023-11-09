

SELECT
_id as product_id,
name as product_name,
case product_type_id
when "63e5ee4fa056b1c6920ed269" then 'Áo'
when "63e5ee4fa056b1c6920ed269" then "Quần"
when "63e5ee4fa056b1c6920ed26b" then "Khác"
else "Chưa phân loại" end as product_type,
keyword as product_keyword,
categories,
updated_at,
created_at,
published_at,
code as product_code,

FROM
    `agile-scheme-394814`.`dbt_dev`.`base_5sfashion__products`