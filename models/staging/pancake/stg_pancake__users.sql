with source as (
    {{ dbt_utils.deduplicate(
    relation=source('pancake', 'users'),
    partition_by='id',
    order_by="_batched_at desc",
   )
}}
)
select 
source.id as user_id, 
source.name as user_name,
source.email as user_email
from source