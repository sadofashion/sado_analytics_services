with source as (
select 
    *
from {{ source('gSheet', 'content') }}
where approval_status is not null
)

{{dbt_utils.deduplicate(
    relation='source',
    partition_by='content_code',
    order_by='_batched_at desc',
)}}