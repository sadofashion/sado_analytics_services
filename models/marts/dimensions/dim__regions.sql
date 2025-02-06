{{
  config(
    materialized = 'view',
    )
}}
select * from {{ ref('regions') }}