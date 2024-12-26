{% macro update_cogs_valid_date() %}

update  {{"dbt_"+target.name}}.stg_gsheet__cogs 
set dbt_valid_from = timestamp('2024-10-01')
where product_code in (
  SELECT
  product_code
FROM
  {{"dbt_"+target.name}}.stg_gsheet__cogs
  group by product_code
  having count(product_code) = 1
  and max(dbt_valid_from) > timestamp('2024-10-01')
)

{% endmacro %}