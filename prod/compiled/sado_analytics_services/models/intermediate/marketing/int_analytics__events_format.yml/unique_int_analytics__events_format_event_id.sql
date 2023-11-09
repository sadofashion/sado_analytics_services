
    
    

with dbt_test__target as (

  select event_id as unique_field
  from `agile-scheme-394814`.`dbt_dev`.`int_analytics__events_format`
  where event_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


