{{
  config(
    materialized = 'view',
    )
}}
select * from {{ ref('provinces') }}