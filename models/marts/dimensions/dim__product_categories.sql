{{
  config(
    materialized = 'view',
    )
}}

select * from {{ ref('product_categories') }}