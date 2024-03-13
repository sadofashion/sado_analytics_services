WITH source AS (
        {{ dbt_utils.deduplicate(
        relation = source(
            'kiotViet',
            'p_users_list'
        ),
        partition_by = 'id',
        order_by = "_batched_at desc",
    ) }}
)
SELECT
    id,
    userName,
    givenName,
    birthDate,
FROM
    source
