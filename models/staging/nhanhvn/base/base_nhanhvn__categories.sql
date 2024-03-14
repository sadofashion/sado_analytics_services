{{ dbt_utils.deduplicate(
    relation = source(
        'nhanhvn',
        'p_categories'
    ),
    partition_by = 'id',
    order_by = "_batched_at desc",
) }}
