WITH source AS (
    {{ dbt_utils.deduplicate(relation = source('pancake', 'customer_engagements'), partition_by = 'hour,page_id', order_by = "_batched_at desc",) }}
)
SELECT
source.comment,
source.hour,
source.inbox,
source.new_customer,
source.new_customer_from_inbox,
source.old_order_count,
source.order_count,
source.total,
source.page_id,
FROM
    source