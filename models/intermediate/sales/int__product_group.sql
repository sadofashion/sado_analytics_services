 select distinct 
    category_name,
    lower(product_group_code) product_group_code,
    product_group,
    season,
    from 
    {{ ref("stg__ad_product_categories") }}