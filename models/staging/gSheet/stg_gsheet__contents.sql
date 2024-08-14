with source as (
select 
    *, 
    parse_date('%Y%m%d',_TABLE_SUFFIX) as content_month
from {{ source('gSheet', 'content') }}
where approval_status = 'Posted'
)

{{dbt_utils.deduplicate(
    relation='source',
    partition_by='content_code',
    order_by='_batched_at desc',
)}}