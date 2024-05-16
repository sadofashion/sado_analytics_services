WITH source AS (
    {{ dbt_utils.deduplicate(relation = source('pancake', 'customers'), partition_by = 'id, psid', order_by = "_batched_at desc",) }}
)
SELECT
    source.id AS customer_id,
    source.name AS customer_name,
    source.gender,
    date_add(
        source.inserted_at,
        INTERVAL 7 HOUR
    ) inserted_at,
    source.phone_numbers,
    source.lives_in AS city,
    source.psid AS customer_facebook_id,
    source.recent_orders,
FROM
    source
