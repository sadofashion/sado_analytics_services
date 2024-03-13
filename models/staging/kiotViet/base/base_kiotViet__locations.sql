WITH source AS (
        {{ dbt_utils.deduplicate(
        relation = source(
            'kiotViet',
            'p_locations_list'
        ),
        partition_by = 'id',
        order_by = "_batched_at desc",
    ) }}
)
SELECT
    id,
    name,
    normalName
from source
