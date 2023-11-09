WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                _batched_at DESC
        ) AS rn_
    FROM
        {{ source(
            'kiotViet',
            'p_locations_list_*'
        ) }}
)
SELECT
    id,
    name,
    normalName
from source
WHERE
    rn_ = 1
