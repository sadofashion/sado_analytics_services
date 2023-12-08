WITH source AS (
    {{ dbt_utils.deduplicate(relation = source('pancake', 'page_stats'), partition_by = 'hour,page_id', order_by = "_batched_at desc",) }}
)
SELECT
source.customer_comment_count,
source.customer_inbox_count,
source.hour,
source.inbox_interactive_count,
source.new_customer_count,
source.new_inbox_count,
source.page_comment_count,
source.page_inbox_count,
source.phone_number_count,
source.today_uniq_website_referral,
source.today_website_guest_referral,
source.uniq_phone_number_count,
source.page_id,
FROM
    source