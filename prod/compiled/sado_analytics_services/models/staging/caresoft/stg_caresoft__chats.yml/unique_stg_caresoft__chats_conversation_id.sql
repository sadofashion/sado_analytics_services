
    
    

with dbt_test__target as (

  select conversation_id as unique_field
  from `agile-scheme-394814`.`dbt_dev`.`stg_caresoft__chats`
  where conversation_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


