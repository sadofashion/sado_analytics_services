
    
    

with all_values as (

    select
        device_type as value_field,
        count(*) as n_records

    from `agile-scheme-394814`.`dbt_dev`.`int_analytics__events_format`
    group by device_type

)

select *
from all_values
where value_field not in (
    'desktop','mobile','tablet'
)


