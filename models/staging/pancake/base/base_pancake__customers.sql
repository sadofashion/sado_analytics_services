WITH source AS (
    {{ dbt_utils.deduplicate(relation = source('pancake', 'customers'), partition_by = 'id, psid', order_by = "_batched_at desc",) }}
)
SELECT
source.id as customer_id,
source.name as customer_name,
source.gender,
date_add(source.inserted_at, interval 7 hour) inserted_at,
source.phone_numbers,
source.lives_in as city,
source.psid as customer_facebook_id,
source.recent_orders,
FROM
    source