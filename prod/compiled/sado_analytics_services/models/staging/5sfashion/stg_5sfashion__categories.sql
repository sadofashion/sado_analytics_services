

SELECT
  _id AS category_id,
  name AS category_name,
  product_order_type,
  position as homepage_position,
  is_featured as is_show_on_homepage,
  is_show_on_website,
FROM
    `agile-scheme-394814`.`dbt_dev`.`base_5sfashion__categories`