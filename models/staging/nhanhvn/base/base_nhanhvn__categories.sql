{{ dbt_utils.deduplicate(
    relation = source(
        'nhanhvn',
        'p_categories_*'
    ),
    partition_by = 'id',
    order_by = "_batched_at desc",
) }}
