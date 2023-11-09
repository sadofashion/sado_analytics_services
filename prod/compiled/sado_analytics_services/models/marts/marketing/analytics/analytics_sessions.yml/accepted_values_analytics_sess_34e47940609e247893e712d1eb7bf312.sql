
    
    

with all_values as (

    select
        channel_grouping as value_field,
        count(*) as n_records

    from `agile-scheme-394814`.`dbt_dev_marketing`.`analytics_sessions`
    group by channel_grouping

)

select *
from all_values
where value_field not in (
    'Paid Social','Organic Social','Direct','Organic Search','Referral','Paid Search','Display','Unassigned'
)


