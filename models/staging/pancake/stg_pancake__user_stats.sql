{{
  config(
    tags=['view', 'fact','pancake']
  )
}}

WITH source AS (
    {{ dbt_utils.deduplicate(relation = source('pancake', 'user_stats'), partition_by = 'page_id,user_id,hour', order_by = "_batched_at desc",) }}
)
SELECT
source.hour,
source.page_id,
source.user_id,
source.average_response_time,
source.comment_count,
source.hour_in_integer,
source.inbox_count,
source.order_count,
source.phone_number_count,
source.private_reply_count,
source.unique_comment_count,
source.unique_inbox_count,
FROM
    source